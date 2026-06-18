import os
import time
from decimal import Decimal
from typing import List, Literal, Optional, Tuple

from pydantic import Field, field_validator

from controllers.market_making.dex_cex_quoting import compute_regime_order_price
from controllers.market_making.dex_cex_regime import update_regime_with_hysteresis
from controllers.market_making.dex_cex_utils import WarningThrottler
from controllers.market_making.dex_price_feed import DexPriceFeed, DexPriceFeedConfig, TwapSource, compute_basis_pct
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.market_making_controller_base import (
    MarketMakingControllerBase,
    MarketMakingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction

# Phase 0 — ALI / WETH Uniswap V3 pool (Ethereum mainnet)
DEFAULT_ALI_TOKEN = "0x6B0b3a982b4634aC68dD83a4DBF02311cE324181"
DEFAULT_WETH_TOKEN = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
DEFAULT_POOL_ADDRESS = "0xF260d15e8eBe54D210ef53F5b61Cb46bD9Aa29EE"

WARN_THROTTLE_SECONDS = 60.0
STATUS_LOG_THROTTLE_SECONDS = 60.0
DEBUG_LOG_THROTTLE_SECONDS = 30.0


class PMMDynamicControllerConfig(MarketMakingControllerConfigBase):
    controller_name: str = "pmm_dynamic"
    candles_config: List[CandlesConfig] = Field(default=[])

    dex_rpc_url: str = Field(
        default="",
        json_schema_extra={
            "prompt": "Ethereum RPC URL (or set DEX_RPC_URL env): ",
            "prompt_on_new": True,
        },
    )
    dex_pool_address: str = Field(
        default=DEFAULT_POOL_ADDRESS,
        json_schema_extra={"prompt": "Uniswap V3 pool address: ", "prompt_on_new": True},
    )
    dex_base_token_address: str = Field(
        default=DEFAULT_ALI_TOKEN,
        json_schema_extra={"prompt": "Base token address (ALI): ", "prompt_on_new": True},
    )
    dex_quote_token_address: str = Field(
        default=DEFAULT_WETH_TOKEN,
        json_schema_extra={"prompt": "Quote token address (WETH): ", "prompt_on_new": True},
    )
    dex_eth_usdt_trading_pair: str = Field(
        default="ETH-USDT",
        json_schema_extra={
            "prompt": "ETH/USDT pair on the MM CEX (same connector): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    dex_poll_interval_seconds: int = Field(
        default=2,
        json_schema_extra={"prompt": "DEX feed poll interval (seconds): ", "prompt_on_new": True},
    )
    dex_twap_seconds: int = Field(
        default=180,
        json_schema_extra={"prompt": "Uniswap observe() TWAP window (seconds): ", "prompt_on_new": True},
    )
    dex_price_max_stale_seconds: int = Field(
        default=30,
        json_schema_extra={"prompt": "Max seconds without DEX feed before stale: ", "prompt_on_new": True},
    )
    dex_sanity_max_divergence_pct: Decimal = Field(
        default=Decimal("0.15"),
        json_schema_extra={
            "prompt": "Max |cex-dex|/dex for broken-feed guard (e.g. 0.15): ",
            "prompt_on_new": True,
        },
    )
    regime_hysteresis_bps: int = Field(
        default=50,
        json_schema_extra={
            "prompt": "Regime switch threshold in bps (e.g. 50 = 0.5%): ",
            "prompt_on_new": True,
        },
    )
    regime_hysteresis_ticks: int = Field(
        default=2,
        json_schema_extra={
            "prompt": "Consecutive ticks beyond threshold to switch regime: ",
            "prompt_on_new": True,
        },
    )
    dex_cex_log_only: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Log-only mode (CEX quotes, log dex_fair/regime): true/false ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    dex_cex_debug: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Verbose DEX/CEX debug logs (feed + order path): true/false ",
            "prompt_on_new": False,
            "is_updatable": True,
        },
    )

    @field_validator("dex_rpc_url", mode="before")
    @classmethod
    def resolve_dex_rpc_url(cls, v):
        if v is None or str(v).strip() == "":
            v = os.getenv("DEX_RPC_URL") or os.getenv("WEB3_PROVIDER")
        if not v:
            raise ValueError("dex_rpc_url is required (or set DEX_RPC_URL / WEB3_PROVIDER)")
        return str(v).strip()

    @field_validator("dex_sanity_max_divergence_pct", mode="before")
    @classmethod
    def parse_dex_sanity(cls, v):
        if isinstance(v, str):
            if v == "":
                return Decimal("0.15")
            return Decimal(v)
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        return v


class PMMDynamicController(MarketMakingControllerBase):
    """
    CEX + DEX asymmetric market making.

    Uses Uniswap V3 pool TWAP (observe) × CEX ETH/USDT for dex_fair, then Regime A/B quoting.
    Phase 2: dex_cex_log_only=True keeps CEX mid ± spreads while logging feed/regime.
    Phase 3: dex_cex_log_only=False uses Regime A/B anchors; skips levels when feed stale or sanity fails.
    Reuses last successful DEX TWAP for quoting when the current poll fails (within stale limit).
    """

    def __init__(self, config: PMMDynamicControllerConfig, *args, **kwargs):
        self.config = config
        self._regime: Optional[Literal["A", "B"]] = None
        self._regime_confirm_count: int = 0
        self._last_logged_regime: Optional[str] = None
        self._dex_feed: Optional[DexPriceFeed] = None
        self._warn_throttle = WarningThrottler(WARN_THROTTLE_SECONDS)
        self._status_log_throttle = WarningThrottler(STATUS_LOG_THROTTLE_SECONDS)
        self._debug_log_throttle = WarningThrottler(DEBUG_LOG_THROTTLE_SECONDS)
        super().__init__(config, *args, **kwargs)
        self.market_data_provider.initialize_rate_sources([
            ConnectorPair(
                connector_name=config.connector_name,
                trading_pair=config.trading_pair,
            ),
            ConnectorPair(
                connector_name=config.connector_name,
                trading_pair=config.dex_eth_usdt_trading_pair,
            ),
        ])

    def _debug(self, key: str, msg: str, *args, now: Optional[float] = None) -> None:
        if not self.config.dex_cex_debug:
            return
        now = now or time.time()
        if self._debug_log_throttle.should_log(key, now):
            self.logger().info(msg, *args)

    def _eth_usdt_mid_available(self) -> bool:
        """CEX ETH/USDT order book must exist before DexPriceFeed can poll."""
        try:
            connector = self.market_data_provider.get_connector(self.config.connector_name)
            if not connector.ready:
                self._debug(
                    "eth_usdt_wait",
                    "[DEX/CEX] %s not ready for feed: %s",
                    self.config.connector_name,
                    connector.status_dict,
                )
                return False
            self.market_data_provider.get_price_by_type(
                self.config.connector_name,
                self.config.dex_eth_usdt_trading_pair,
                PriceType.MidPrice,
            )
            return True
        except Exception as e:
            self._debug("eth_usdt_fail", "[DEX/CEX] %s mid unavailable: %s", self.config.dex_eth_usdt_trading_pair, e)
            return False

    def _ensure_dex_feed(self, now: float) -> bool:
        """
        Lazy-init DexPriceFeed after the MM connector is ready (avoids startup race when
        the controller is added before CEX order books are loaded). Retries each tick.
        """
        if self._dex_feed is not None:
            return True
        if not self._eth_usdt_mid_available():
            if self._warn_throttle.should_log("dex_feed_wait_cex", now):
                self.logger().warning(
                    "DexPriceFeed waiting for %s on %s (connector or order book not ready yet)",
                    self.config.dex_eth_usdt_trading_pair,
                    self.config.connector_name,
                )
            return False
        feed_config = DexPriceFeedConfig(
            dex_rpc_url=self.config.dex_rpc_url,
            dex_pool_address=self.config.dex_pool_address,
            base_token_address=self.config.dex_base_token_address,
            quote_token_address=self.config.dex_quote_token_address,
            connector_name=self.config.connector_name,
            dex_eth_usdt_trading_pair=self.config.dex_eth_usdt_trading_pair,
            dex_twap_seconds=self.config.dex_twap_seconds,
            dex_poll_interval_seconds=self.config.dex_poll_interval_seconds,
            dex_price_max_stale_seconds=self.config.dex_price_max_stale_seconds,
            dex_sanity_max_divergence_pct=self.config.dex_sanity_max_divergence_pct,
            verbose_logging=self.config.dex_cex_debug,
        )
        try:
            self._dex_feed = DexPriceFeed(
                config=feed_config,
                market_data_provider=self.market_data_provider,
                logger=self.logger(),
            )
            eth_mid = self._dex_feed.get_eth_usdt_mid()
        except Exception as e:
            self._dex_feed = None
            if self._warn_throttle.should_log("dex_feed_init_fail", now):
                self.logger().warning(
                    "DexPriceFeed init failed (%s on %s): %s",
                    self.config.dex_eth_usdt_trading_pair,
                    self.config.connector_name,
                    e,
                )
            return False
        self.logger().info(
            "DexPriceFeed initialized; %s mid=%s pool=%s twap=%ss",
            self.config.dex_eth_usdt_trading_pair,
            eth_mid,
            self.config.dex_pool_address,
            self.config.dex_twap_seconds,
        )
        return True

    def _update_regime(self, basis_pct: Optional[Decimal]) -> Optional[Literal["A", "B"]]:
        old = self._regime
        self._regime, self._regime_confirm_count = update_regime_with_hysteresis(
            basis_pct,
            self._regime,
            self._regime_confirm_count,
            self.config.regime_hysteresis_bps,
            self.config.regime_hysteresis_ticks,
        )
        if old is not None and self._regime != old and basis_pct is not None:
            self.logger().info(
                "Regime %s -> %s (basis_pct=%s)",
                old,
                self._regime,
                f"{basis_pct * 100:.4f}%",
            )
        elif (
            old is None
            and self._regime is not None
            and basis_pct is not None
            and not self.config.dex_cex_log_only
        ):
            self.logger().info(
                "Regime initialized -> %s (basis_pct=%s)",
                self._regime,
                f"{basis_pct * 100:.4f}%",
            )
        return self._regime

    def _hypothetical_prices(self, level: int = 0) -> dict:
        cex_mid = self.processed_data.get("cex_mid")
        dex_fair = self.processed_data.get("dex_fair")
        if cex_mid is None or dex_fair is None:
            return {}
        buy_spreads, _ = self.config.get_spreads_and_amounts_in_quote(TradeType.BUY)
        sell_spreads, _ = self.config.get_spreads_and_amounts_in_quote(TradeType.SELL)
        buy_s = Decimal(str(buy_spreads[level]))
        sell_s = Decimal(str(sell_spreads[level]))
        return {
            "regime_a_buy": dex_fair * (1 - buy_s),
            "regime_a_sell": cex_mid * (1 + sell_s),
            "regime_b_buy": cex_mid * (1 - buy_s),
            "regime_b_sell": dex_fair * (1 + sell_s),
        }

    def _log_feed_status(self, now: float) -> None:
        pd = self.processed_data
        regime = pd.get("regime")
        stale = pd.get("feed_stale")

        if self.config.dex_cex_debug and self._debug_log_throttle.should_log("dex_cex_heartbeat", now):
            basis = pd.get("basis_pct")
            basis_str = f"{basis * 100:.4f}%" if basis is not None else None
            self.logger().info(
                "[DEX/CEX] heartbeat log_only=%s regime=%s cex_mid=%s dex_fair=%s basis=%s "
                "twap_source=%s stale=%s sanity_ok=%s eth_usdt=%s mdp_ready=%s",
                self.config.dex_cex_log_only,
                regime,
                pd.get("cex_mid"),
                pd.get("dex_fair"),
                basis_str,
                pd.get("twap_source"),
                stale,
                pd.get("feed_sanity_ok"),
                pd.get("eth_usdt_mid"),
                self.market_data_provider.ready,
            )

        if regime == self._last_logged_regime and not stale:
            return
        if stale and regime == self._last_logged_regime:
            if not self._status_log_throttle.should_log("dex_cex_status_stale", now):
                return
        self._last_logged_regime = regime
        hypo = self._hypothetical_prices(0)
        self.logger().info(
            "[DEX/CEX] log_only=%s regime=%s cex_mid=%s dex_fair=%s basis=%s "
            "twap_source=%s stale=%s sanity_ok=%s | L0 A: buy=%s sell=%s | L0 B: buy=%s sell=%s",
            self.config.dex_cex_log_only,
            regime,
            pd.get("cex_mid"),
            pd.get("dex_fair"),
            f"{pd.get('basis_pct') * 100:.4f}%" if pd.get("basis_pct") is not None else None,
            pd.get("twap_source"),
            stale,
            pd.get("feed_sanity_ok"),
            hypo.get("regime_a_buy"),
            hypo.get("regime_a_sell"),
            hypo.get("regime_b_buy"),
            hypo.get("regime_b_sell"),
        )

    def _log_feed_warnings(
        self,
        now: float,
        snap,
        feed_stale: bool,
        feed_sanity_ok: bool,
        dex_fair: Optional[Decimal],
    ) -> None:
        if snap and snap.twap_source == TwapSource.FALLBACK:
            if self._warn_throttle.should_log("twap_fallback", now):
                self.logger().warning("DexPriceFeed using fallback TWAP (observe unavailable)")
        if feed_stale and self._warn_throttle.should_log("feed_stale", now):
            self.logger().warning(
                "DexPriceFeed stale (last success > %ss ago)",
                self.config.dex_price_max_stale_seconds,
            )
        if dex_fair is not None and not feed_sanity_ok:
            if self._warn_throttle.should_log("sanity_fail", now):
                self.logger().warning(
                    "DexPriceFeed sanity check failed (|cex-dex|/dex > %s)",
                    self.config.dex_sanity_max_divergence_pct,
                )

    async def update_processed_data(self):
        now = time.time()
        try:
            cex_mid = Decimal(
                str(
                    self.market_data_provider.get_price_by_type(
                        self.config.connector_name,
                        self.config.trading_pair,
                        PriceType.MidPrice,
                    )
                )
            )
        except Exception as e:
            self.logger().error("Failed to get cex_mid for %s: %s", self.config.trading_pair, e)
            self.processed_data = {
                "reference_price": Decimal("0"),
                "cex_mid": None,
                "dex_fair": None,
                "basis_pct": None,
                "regime": self._regime,
                "feed_stale": True,
                "feed_sanity_ok": False,
                "twap_source": self._last_twap_source(),
            }
            return

        feed_ready = self._ensure_dex_feed(now)
        if not feed_ready:
            self._debug("feed_not_ready", "[DEX/CEX] feed not ready (waiting for ETH-USDT or RPC)", now=now)
        elif self._dex_feed.should_poll(now):
            self._dex_feed.poll(cex_mid=cex_mid)
        else:
            self._debug(
                "poll_skip",
                "[DEX/CEX] poll skipped (interval %ss)",
                self.config.dex_poll_interval_seconds,
                now=now,
            )

        snap = self._dex_feed.snapshot if self._dex_feed else None
        dex_fair = self._dex_feed.get_quoting_dex_fair(now) if self._dex_feed else None
        basis_pct = compute_basis_pct(cex_mid, dex_fair)
        regime = self._update_regime(basis_pct)
        feed_stale = self._dex_feed.is_stale(now) if self._dex_feed else True
        feed_sanity_ok = self._dex_feed.sanity_ok(cex_mid, now) if self._dex_feed else False

        self._log_feed_warnings(now, snap, feed_stale, feed_sanity_ok, dex_fair)

        if (
            not self.config.dex_cex_log_only
            and regime is None
            and dex_fair is not None
            and basis_pct is not None
            and self._warn_throttle.should_log("regime_unset", now)
        ):
            self.logger().warning(
                "Regime unset (basis_pct=%s); no Regime A/B quotes until |basis| > %s bps",
                f"{basis_pct * 100:.4f}%",
                self.config.regime_hysteresis_bps,
            )

        self.processed_data = {
            "reference_price": cex_mid,
            "cex_mid": cex_mid,
            "dex_fair": dex_fair,
            "basis_pct": basis_pct,
            "regime": regime,
            "feed_stale": feed_stale,
            "feed_sanity_ok": feed_sanity_ok,
            "twap_source": (
                self._dex_feed.get_quoting_twap_source().value
                if self._dex_feed
                else TwapSource.NONE.value
            ),
            "eth_usdt_mid": snap.eth_usdt_mid if snap else None,
            "spread_multiplier": Decimal("1"),
        }
        self._log_feed_status(now)

    def _last_twap_source(self) -> str:
        if self._dex_feed is not None:
            return self._dex_feed.snapshot.twap_source.value
        return TwapSource.NONE.value

    def get_price_and_amount(self, level_id: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        if self.config.dex_cex_log_only:
            return MarketMakingControllerBase.get_price_and_amount(self, level_id)
        return self._get_regime_price_and_amount(level_id)

    def _get_regime_price_and_amount(
        self, level_id: str
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        level = self.get_level_from_level_id(level_id)
        trade_type = self.get_trade_type_from_level_id(level_id)
        spreads, amounts_quote = self.config.get_spreads_and_amounts_in_quote(trade_type)
        spread = Decimal(str(spreads[int(level)]))

        cex_mid = self.processed_data.get("cex_mid")
        dex_fair = self.processed_data.get("dex_fair")
        regime = self.processed_data.get("regime")

        if (
            cex_mid is None
            or dex_fair is None
            or regime is None
            or self.processed_data.get("feed_stale")
            or not self.processed_data.get("feed_sanity_ok")
        ):
            self._debug(
                f"regime_price_skip_{level_id}",
                "[DEX/CEX ORDER SKIP] %s live quote blocked: cex_mid=%s dex_fair=%s regime=%s "
                "stale=%s sanity_ok=%s",
                level_id,
                cex_mid,
                dex_fair,
                regime,
                self.processed_data.get("feed_stale"),
                self.processed_data.get("feed_sanity_ok"),
            )
            return None, None

        order_price = compute_regime_order_price(
            regime=regime,
            is_buy=trade_type == TradeType.BUY,
            spread=spread,
            cex_mid=cex_mid,
            dex_fair=dex_fair,
        )
        return order_price, Decimal(amounts_quote[int(level)]) / order_price

    def get_levels_to_execute(self) -> List[str]:
        if not self.config.dex_cex_log_only:
            if self.processed_data.get("feed_stale") or not self.processed_data.get("feed_sanity_ok"):
                self._debug(
                    "levels_blocked",
                    "[DEX/CEX] no levels (live mode): stale=%s sanity_ok=%s dex_fair=%s regime=%s",
                    self.processed_data.get("feed_stale"),
                    self.processed_data.get("feed_sanity_ok"),
                    self.processed_data.get("dex_fair"),
                    self.processed_data.get("regime"),
                )
                return []
        levels = super().get_levels_to_execute()
        self._debug(
            "levels",
            "[DEX/CEX] levels_to_execute=%s active_executors=%s",
            levels,
            len(self.executors_info),
        )
        return levels

    def _log_live_quote(self, level_id: str, price: Decimal, amount: Decimal, now: float) -> None:
        if self.config.dex_cex_log_only:
            return
        if not self._warn_throttle.should_log(f"live_quote_{level_id}", now):
            return
        pd = self.processed_data
        basis = pd.get("basis_pct")
        basis_str = f"{basis * 100:.4f}%" if basis is not None else None
        self.logger().info(
            "[DEX/CEX QUOTE] %s regime=%s price=%s amount=%s cex_mid=%s dex_fair=%s basis=%s",
            level_id,
            pd.get("regime"),
            price,
            amount,
            pd.get("cex_mid"),
            pd.get("dex_fair"),
            basis_str,
        )

    def create_actions_proposal(self) -> List[ExecutorAction]:
        create_actions = []
        now = time.time()
        position_rebalance_action = self.check_position_rebalance()
        if position_rebalance_action:
            self._debug("rebalance", "[DEX/CEX] proposing position rebalance", now=now)
            create_actions.append(position_rebalance_action)

        levels = self.get_levels_to_execute()
        if not levels:
            self._debug("no_levels", "[DEX/CEX] create_actions: no levels to execute", now=now)

        for level_id in levels:
            price, amount = self.get_price_and_amount(level_id)
            if price is None or amount is None or price <= 0 or amount <= 0:
                self.logger().info(
                    "[DEX/CEX ORDER SKIP] %s log_only=%s price=%s amount=%s ref=%s",
                    level_id,
                    self.config.dex_cex_log_only,
                    price,
                    amount,
                    self.processed_data.get("reference_price"),
                )
                continue
            self._log_live_quote(level_id, price, amount, now)
            mode = "CEX" if self.config.dex_cex_log_only else "REGIME"
            self.logger().info(
                "[DEX/CEX ORDER] %s mode=%s price=%s amount=%s cex_mid=%s dex_fair=%s regime=%s",
                level_id,
                mode,
                price,
                amount,
                self.processed_data.get("cex_mid"),
                self.processed_data.get("dex_fair"),
                self.processed_data.get("regime"),
            )
            executor_config = self.get_executor_config(level_id, price, amount)
            if executor_config is not None:
                create_actions.append(
                    CreateExecutorAction(
                        controller_id=self.config.id,
                        executor_config=executor_config,
                    )
                )
        self._debug("actions", "[DEX/CEX] create_actions count=%s", len(create_actions), now=now)
        return create_actions

    async def control_task(self):
        if self.config.dex_cex_debug and not self.market_data_provider.ready:
            self._debug(
                "mdp_not_ready",
                "[DEX/CEX] controller tick skipped: market_data_provider.ready=False",
            )
        elif self.config.dex_cex_debug and not self.executors_update_event.is_set():
            self._debug(
                "executors_event",
                "[DEX/CEX] controller tick skipped: waiting for executor update event",
            )
        await super().control_task()

    def get_executor_config(
        self,
        level_id: str,
        price: Decimal,
        amount: Decimal,
    ) -> Optional[PositionExecutorConfig]:
        if price is None or amount is None or price <= 0 or amount <= 0:
            return None
        trade_type = self.get_trade_type_from_level_id(level_id)
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            level_id=level_id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_price=price,
            amount=amount,
            triple_barrier_config=self.config.triple_barrier_config,
            leverage=self.config.leverage,
            side=trade_type,
        )

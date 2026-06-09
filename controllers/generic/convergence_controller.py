"""
Convergence controller — spawns ConvergenceExecutor when CEX mid diverges from DEX TWAP.

Runs alongside PMM and arbitrage in the same v2_with_controllers script.
"""
import asyncio
import os
from decimal import Decimal
from typing import List, Literal, Optional

import pandas as pd
from pydantic import Field, field_validator, model_validator

from controllers.market_making.dex_price_feed import DexPriceFeed, DexPriceFeedConfig, compute_basis_pct
from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import MarketDict, PriceType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.convergence_executor.data_types import ConvergenceExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class ConvergenceControllerConfig(ControllerConfigBase):
    controller_name: str = "convergence_controller"
    candles_config: List[CandlesConfig] = []

    connector_name: str = "gate_io"
    trading_pair: str = "ALI-USDT"

    dex_rpc_url: Optional[str] = None
    dex_pool_address: str
    dex_base_token_address: str
    dex_quote_token_address: str
    dex_eth_usdt_trading_pair: str = "ETH-USDT"
    dex_poll_interval_seconds: int = 2
    dex_twap_seconds: int = 180
    dex_price_max_stale_seconds: int = 30
    dex_sanity_max_divergence_pct: Decimal = Decimal("0.15")

    min_gap_bps: Decimal = Decimal("30")
    require_gap_widening: bool = True
    rebalance_only: bool = True
    min_edge_bps_after_fees: Decimal = Decimal("10")
    max_amount_quote: Decimal = Decimal("50")
    zone_quote_threshold_usdt: Decimal = Decimal("100")
    max_balance_pct: Decimal = Decimal("0.5")
    max_walk_levels: int = Field(default=8, ge=1)
    sweep_chunks: int = Field(default=3, ge=1)
    allowed_sides: Literal["both", "buy", "sell"] = "both"
    cooldown_time: int = 60
    skip_if_arb_active: bool = True
    debug_verbose: bool = False

    @model_validator(mode="after")
    def resolve_dex_rpc_url(self):
        if self.dex_rpc_url is None or str(self.dex_rpc_url).strip() == "":
            url = os.getenv("DEX_RPC_URL") or os.getenv("WEB3_PROVIDER")
            if not url:
                raise ValueError("dex_rpc_url is required (or set DEX_RPC_URL / WEB3_PROVIDER)")
            object.__setattr__(self, "dex_rpc_url", str(url).strip())
        return self

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

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets = markets.add_or_update(self.connector_name, self.trading_pair)
        return markets.add_or_update(self.connector_name, self.dex_eth_usdt_trading_pair)


class ConvergenceController(ControllerBase):
    def __init__(self, config: ConvergenceControllerConfig, *args, **kwargs):
        self.config = config
        self._dex_feed: Optional[DexPriceFeed] = None
        self._last_spawn_timestamp: float = 0.0
        self._last_skip_reason: Optional[str] = None
        self._last_logged_skip_key: Optional[str] = None
        self._last_logged_skip_ts: float = 0.0
        super().__init__(config, *args, **kwargs)

    _SPAWN_SKIP_LOG_INTERVAL_SEC: float = 30.0

    def _log_spawn_skip(self, reason: str, **kwargs) -> None:
        """Throttled INFO log when the controller does not spawn an executor."""
        now = self.market_data_provider.time()
        key = reason + "|" + "|".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
        if (
            key == self._last_logged_skip_key
            and now - self._last_logged_skip_ts < self._SPAWN_SKIP_LOG_INTERVAL_SEC
        ):
            if not self.config.debug_verbose:
                return
        self._last_logged_skip_key = key
        self._last_logged_skip_ts = now
        parts = " ".join(f"{k}={v}" for k, v in kwargs.items())
        self.logger().info(
            "Convergence controller skip: %s %s",
            reason,
            parts,
        )

    def _ensure_dex_feed(self) -> bool:
        if self._dex_feed is not None:
            return True
        if not self.market_data_provider.ready:
            self._last_skip_reason = "market data provider not ready"
            return False
        try:
            self._dex_feed = DexPriceFeed(
                DexPriceFeedConfig(
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
                ),
                self.market_data_provider,
                logger=self.logger(),
            )
            return True
        except Exception as e:
            self._last_skip_reason = f"DexPriceFeed init failed: {e}"
            return False

    def _cex_mid(self) -> Optional[Decimal]:
        try:
            return self.market_data_provider.get_price_by_type(
                self.config.connector_name,
                self.config.trading_pair,
                PriceType.MidPrice,
            )
        except Exception as e:
            self._last_skip_reason = f"cex mid failed: {e}"
            return None

    def _refresh_dex_fair_if_needed(self, cex_mid: Decimal) -> None:
        """H-2: ensure spawn decisions use a fresh dex_fair, not only update_processed_data poll."""
        if self._dex_feed is None:
            return
        if (
            self._dex_feed.get_dex_fair() is None
            or self._dex_feed.is_stale()
            or self._dex_feed.should_poll()
        ):
            self._dex_feed.poll(cex_mid=cex_mid)

    def _active_convergence_executors(self):
        return [
            e for e in self.executors_info
            if e.type == "convergence_executor" and e.status != RunnableStatus.TERMINATED
        ]

    def _arb_active_on_pair(self) -> bool:
        if not self.config.skip_if_arb_active:
            return False
        for e in self.executors_info:
            if e.type != "arbitrage_executor" or e.status == RunnableStatus.TERMINATED:
                continue
            cfg = e.config
            buy_pair = getattr(cfg, "buying_market", None)
            sell_pair = getattr(cfg, "selling_market", None)
            for cp in (buy_pair, sell_pair):
                if cp is None:
                    continue
                if (cp.connector_name == self.config.connector_name
                        and cp.trading_pair == self.config.trading_pair):
                    return True
        return False

    async def update_processed_data(self):
        if not self._ensure_dex_feed():
            self.processed_data["skip_reason"] = self._last_skip_reason
            return
        cex_mid = self._cex_mid()
        if cex_mid is None:
            self.processed_data["skip_reason"] = self._last_skip_reason
            return
        if self._dex_feed.should_poll():
            snap = await asyncio.to_thread(self._dex_feed.poll, cex_mid=cex_mid)
            basis = compute_basis_pct(cex_mid, snap.dex_fair)
            self.processed_data["cex_mid"] = float(cex_mid)
            self.processed_data["dex_fair"] = float(snap.dex_fair) if snap.dex_fair else None
            self.processed_data["basis_pct"] = float(basis) if basis is not None else None
            self.processed_data["dex_stale"] = self._dex_feed.is_stale()
        else:
            self.processed_data["cex_mid"] = float(cex_mid)
            dex_fair = self._dex_feed.get_dex_fair()
            if dex_fair is not None:
                basis = compute_basis_pct(cex_mid, dex_fair)
                self.processed_data["dex_fair"] = float(dex_fair)
                self.processed_data["basis_pct"] = float(basis) if basis is not None else None
            self.processed_data["dex_stale"] = self._dex_feed.is_stale()
        self.processed_data["skip_reason"] = self._last_skip_reason

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        now = self.market_data_provider.time()

        active = self._active_convergence_executors()
        if active:
            self._log_spawn_skip("active_executor", count=len(active))
            return actions

        cooldown_left = self.config.cooldown_time - (now - self._last_spawn_timestamp)
        if cooldown_left > 0:
            self._log_spawn_skip("cooldown", seconds_left=f"{cooldown_left:.0f}")
            return actions

        if self._arb_active_on_pair():
            self._last_skip_reason = "arb executor active on same CEX pair"
            self._log_spawn_skip("arb_active", pair=self.config.trading_pair)
            return actions

        if not self._ensure_dex_feed():
            self._log_spawn_skip(
                "dex_feed_unavailable",
                reason=self._last_skip_reason or "unknown",
            )
            return actions

        cex_mid = self._cex_mid()
        if cex_mid is None:
            self._log_spawn_skip(
                "cex_mid_unavailable",
                reason=self._last_skip_reason or "unknown",
            )
            return actions

        self._refresh_dex_fair_if_needed(cex_mid)

        if self._dex_feed.is_stale():
            self._last_skip_reason = "dex feed stale"
            self._log_spawn_skip("dex_feed_stale", cex_mid=cex_mid)
            return actions

        dex_fair = self._dex_feed.get_dex_fair()
        if dex_fair is None:
            self._last_skip_reason = "dex_fair unavailable"
            self._log_spawn_skip("dex_fair_unavailable", cex_mid=cex_mid)
            return actions

        if not self._dex_feed.sanity_ok(cex_mid):
            self._last_skip_reason = "dex sanity failed"
            self._log_spawn_skip("dex_sanity_failed", cex_mid=cex_mid, dex_fair=dex_fair)
            return actions

        gap_bps = abs((cex_mid - dex_fair) / dex_fair * Decimal("10000"))
        if gap_bps < self.config.min_gap_bps:
            self._last_skip_reason = f"gap {gap_bps:.1f}bps below min_gap_bps"
            self._log_spawn_skip(
                "gap_below_min",
                gap_bps=f"{gap_bps:.1f}",
                min_gap_bps=self.config.min_gap_bps,
                cex_mid=cex_mid,
                dex_fair=dex_fair,
            )
            return actions

        implied_side = "SELL" if cex_mid > dex_fair else "BUY"
        try:
            config = ConvergenceExecutorConfig(
                timestamp=now,
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                dex_rpc_url=self.config.dex_rpc_url,
                dex_pool_address=self.config.dex_pool_address,
                dex_base_token_address=self.config.dex_base_token_address,
                dex_quote_token_address=self.config.dex_quote_token_address,
                dex_eth_usdt_trading_pair=self.config.dex_eth_usdt_trading_pair,
                dex_poll_interval_seconds=self.config.dex_poll_interval_seconds,
                dex_twap_seconds=self.config.dex_twap_seconds,
                dex_price_max_stale_seconds=self.config.dex_price_max_stale_seconds,
                dex_sanity_max_divergence_pct=self.config.dex_sanity_max_divergence_pct,
                min_gap_bps=self.config.min_gap_bps,
                require_gap_widening=self.config.require_gap_widening,
                spawn_gap_bps=gap_bps,
                rebalance_only=self.config.rebalance_only,
                min_edge_bps_after_fees=self.config.min_edge_bps_after_fees,
                max_amount_quote=self.config.max_amount_quote,
                zone_quote_threshold_usdt=self.config.zone_quote_threshold_usdt,
                max_balance_pct=self.config.max_balance_pct,
                max_walk_levels=self.config.max_walk_levels,
                sweep_chunks=self.config.sweep_chunks,
                allowed_sides=self.config.allowed_sides,
                debug_verbose=self.config.debug_verbose,
            )
            actions.append(CreateExecutorAction(executor_config=config, controller_id=self.config.id))
            self._last_spawn_timestamp = now
            self._last_skip_reason = None
            self._last_logged_skip_key = None
            self.logger().info(
                "Spawning convergence executor: gap_bps=%.1f side=%s cex_mid=%s dex_fair=%s",
                gap_bps, implied_side, cex_mid, dex_fair,
            )
        except Exception as e:
            self.logger().error("Failed to create convergence executor: %s", e)
            self._last_skip_reason = str(e)

        return actions

    def to_format_status(self) -> List[str]:
        if not self.processed_data:
            return []
        df = pd.DataFrame([self.processed_data])
        return [format_df_for_printout(df, table_format="psql")]

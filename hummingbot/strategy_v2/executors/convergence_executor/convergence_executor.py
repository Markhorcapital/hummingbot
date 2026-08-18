"""
ConvergenceExecutor — push CEX price toward DEX TWAP (dex_fair) via one-sided CEX taker sweeps.

DEX is the reference; CEX is the only trading venue. Sweeps may be split into sweep_chunks
with a fresh book/TWAP read between chunks.
"""
import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Union

from controllers.market_making.dex_price_feed import DexPriceFeed, DexPriceFeedConfig
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.convergence_executor.data_types import ConvergenceExecutorConfig
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class ConvergenceExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy: ScriptStrategyBase,
        config: ConvergenceExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 10,
    ):
        super().__init__(
            strategy=strategy,
            connectors=[config.connector_name],
            config=config,
            update_interval=update_interval,
        )
        self.config: ConvergenceExecutorConfig = config
        self._max_retries = max_retries
        self._current_retries = 0

        self._dex_feed: Optional[DexPriceFeed] = None
        self._dex_feed_ready = False

        self._order: Optional[TrackedOrder] = None
        self._chunks_remaining: int = 0
        self._sweep_side: Optional[TradeType] = None
        self._total_filled_base: Decimal = Decimal("0")
        self._total_filled_quote: Decimal = Decimal("0")
        self._last_dex_fair: Optional[Decimal] = None
        self._last_cex_mid: Optional[Decimal] = None
        self._last_gap_bps: Optional[Decimal] = None
        self._skip_reason: Optional[str] = None
        self._sweep_complete = False
        self._cum_fees_quote: Decimal = Decimal("0")
        self._last_wait_log_key: Optional[str] = None
        self._last_wait_log_ts: float = 0.0
        self._widening_ref_bps: Optional[Decimal] = None
        self._abs_gap_at_place: Optional[Decimal] = None

    _WAIT_LOG_INTERVAL_SEC: float = 30.0

    def _controller_id(self) -> str:
        return getattr(self.config, "id", None) or "?"

    @staticmethod
    def _is_positive_finite(val: Optional[Decimal]) -> bool:
        """True when val is a usable positive Decimal (not None/NaN/<=0)."""
        if val is None:
            return False
        if val.is_nan():
            return False
        return val > 0

    @staticmethod
    def _sanitize_quote_amount(val: Optional[Decimal]) -> Decimal:
        if val is None or val.is_nan() or val < 0:
            return Decimal("0")
        return val

    def _chunk_fill_quote(
        self,
        event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent],
        tracked: TrackedOrder,
    ) -> Decimal:
        """Quote notional for a completed chunk; avoid NaN from market-order price fallback."""
        if self._is_positive_finite(event.quote_asset_amount):
            return event.quote_asset_amount
        if tracked.order and self._is_positive_finite(tracked.order.executed_amount_quote):
            return tracked.order.executed_amount_quote
        base = tracked.order.executed_amount_base if tracked.order else Decimal("0")
        avg = tracked.average_executed_price
        if self._is_positive_finite(base) and self._is_positive_finite(avg):
            return base * avg
        return Decimal("0")

    @staticmethod
    def _abs_gap_bps(cex_mid: Optional[Decimal], dex_fair: Optional[Decimal]) -> Optional[Decimal]:
        if cex_mid is None or dex_fair is None or dex_fair <= 0:
            return None
        return abs((cex_mid - dex_fair) / dex_fair * Decimal("10000"))

    def _update_signed_gap_bps(self, cex_mid: Decimal, dex_fair: Decimal) -> None:
        gap = (cex_mid - dex_fair) / dex_fair
        self._last_gap_bps = gap * Decimal("10000")

    def _init_widening_ref(self, cex_mid: Decimal, dex_fair: Decimal) -> None:
        """Reference gap for require_gap_widening: spawn snapshot or live abs at sweep start."""
        if self.config.spawn_gap_bps is not None:
            self._widening_ref_bps = self.config.spawn_gap_bps
        else:
            self._widening_ref_bps = self._abs_gap_bps(cex_mid, dex_fair)

    def _gap_widening_allows_trade(self, cex_mid: Decimal, dex_fair: Decimal) -> bool:
        if not self.config.require_gap_widening:
            return True
        current_abs = self._abs_gap_bps(cex_mid, dex_fair)
        if current_abs is None or self._widening_ref_bps is None:
            return current_abs is not None
        return current_abs > self._widening_ref_bps

    def _log_decision(self, event: str, **kwargs) -> None:
        parts = " ".join(f"{k}={v}" for k, v in kwargs.items() if v is not None)
        self.logger().info("Convergence [%s] %s %s", self._controller_id(), event, parts)

    def _log_skip_finish(
        self,
        reason: str,
        *,
        cex_mid: Optional[Decimal] = None,
        dex_fair: Optional[Decimal] = None,
        side: Optional[TradeType] = None,
        zone_quote: Optional[Decimal] = None,
        **extra,
    ) -> None:
        gap = self._abs_gap_bps(cex_mid, dex_fair)
        self._log_decision(
            "finish_no_trade",
            reason=reason,
            gap_bps=f"{gap:.1f}" if gap is not None else "n/a",
            cex_mid=cex_mid,
            dex_fair=dex_fair,
            side=side,
            zone_quote=zone_quote,
            chunks_left=self._chunks_remaining,
            filled_base=self._total_filled_base,
            **extra,
        )

    def _log_wait(self, reason: str, **kwargs) -> None:
        """Throttled wait/skip log while the executor stays RUNNING without trading."""
        now = time.time()
        key = reason + "|" + "|".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
        if (
            key == self._last_wait_log_key
            and now - self._last_wait_log_ts < self._WAIT_LOG_INTERVAL_SEC
            and not self.config.debug_verbose
        ):
            return
        self._last_wait_log_key = key
        self._last_wait_log_ts = now
        if self.config.debug_verbose:
            self.logger().debug(
                "Convergence [%s] wait %s %s",
                self._controller_id(),
                reason,
                " ".join(f"{k}={v}" for k, v in kwargs.items()),
            )
        else:
            self._log_decision("wait", reason=reason, **kwargs)

    # ------------------------------------------------------------------ feed
    def _ensure_dex_feed(self) -> bool:
        if self._dex_feed is not None:
            return True
        mdp = self._strategy.market_data_provider
        if not mdp.ready:
            self._skip_reason = "market data provider not ready"
            return False
        try:
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
            )
            self._dex_feed = DexPriceFeed(feed_config, mdp, logger=self.logger())
            self._dex_feed_ready = True
            return True
        except Exception as e:
            self._skip_reason = f"DexPriceFeed init failed: {e}"
            self.logger().error(self._skip_reason)
            return False

    def _poll_dex(self, cex_mid: Optional[Decimal]) -> None:
        if self._dex_feed is None:
            return
        if self._dex_feed.should_poll():
            self._dex_feed.poll(cex_mid=cex_mid)

    async def _poll_dex_async(self, cex_mid: Optional[Decimal]) -> None:
        """Run blocking Web3 poll off the asyncio event loop (H-4)."""
        if self._dex_feed is None:
            return
        if self._dex_feed.should_poll():
            await asyncio.to_thread(self._dex_feed.poll, cex_mid=cex_mid)

    def _cex_mid(self) -> Optional[Decimal]:
        try:
            return self.get_price(
                self.config.connector_name,
                self.config.trading_pair,
                PriceType.MidPrice,
            )
        except Exception as e:
            self._skip_reason = f"cex mid failed: {e}"
            return None

    # ------------------------------------------------------------------ zone walk
    @staticmethod
    def _zone_levels(
        order_book,
        side: TradeType,
        dex_fair: Decimal,
    ) -> List[Tuple[Decimal, Decimal]]:
        levels: List[Tuple[Decimal, Decimal]] = []
        if side == TradeType.BUY:
            for row in order_book.ask_entries():
                price = Decimal(str(row.price))
                if price > dex_fair:
                    break
                levels.append((price, Decimal(str(row.amount))))
        else:
            for row in order_book.bid_entries():
                price = Decimal(str(row.price))
                if price < dex_fair:
                    break
                levels.append((price, Decimal(str(row.amount))))
        return levels

    def _estimate_taker_fee_quote(self, side: TradeType, amount_base: Decimal, price: Decimal) -> Decimal:
        base, quote = split_hb_trading_pair(self.config.trading_pair)
        connector = self.connectors[self.config.connector_name]
        fee = connector.get_fee(
            base_currency=base,
            quote_currency=quote,
            order_type=OrderType.MARKET,
            order_side=side,
            amount=amount_base,
            price=price,
            is_maker=False,
        )
        return fee.fee_amount_in_token(
            trading_pair=self.config.trading_pair,
            price=price,
            order_amount=amount_base,
            token=quote,
            exchange=connector,
        )

    def _max_quote_per_chunk(self) -> Decimal:
        max_quote = self.config.max_amount_quote
        if self._chunks_remaining > 0 and self.config.sweep_chunks > 0:
            max_quote = max_quote / Decimal(self.config.sweep_chunks)
        return max_quote

    def _apply_balance_and_min_size(
        self,
        size: Decimal,
        vwap: Decimal,
        side: TradeType,
    ) -> Tuple[Decimal, Decimal]:
        reserve = self.config.max_balance_pct
        base_asset, quote_asset = split_hb_trading_pair(self.config.trading_pair)
        connector = self.connectors[self.config.connector_name]

        if side == TradeType.SELL:
            avail = Decimal(str(connector.get_available_balance(base_asset))) * reserve
            if size > avail:
                size = avail
        else:
            avail_quote = Decimal(str(connector.get_available_balance(quote_asset))) * reserve
            if size * vwap > avail_quote and vwap > 0:
                size = avail_quote / vwap

        trading_rules = self.get_trading_rules(self.config.connector_name, self.config.trading_pair)
        size = connector.quantize_order_amount(self.config.trading_pair, size)
        if size < trading_rules.min_order_size:
            return Decimal("0"), vwap
        return size, vwap

    def _compute_rebalance_chunk_size(
        self, dex_fair: Decimal, side: TradeType
    ) -> Tuple[Decimal, Decimal, Decimal]:
        """Size from zone liquidity up to per-chunk quote cap (no profit gate)."""
        book = self.get_order_book(self.config.connector_name, self.config.trading_pair)
        levels = self._zone_levels(book, side, dex_fair)
        if not levels:
            return Decimal("0"), Decimal("0"), Decimal("0")

        max_quote = self._max_quote_per_chunk()
        max_levels = max(1, int(self.config.max_walk_levels))
        cum_size = Decimal("0")
        cum_cost = Decimal("0")

        for level_price, level_amount in levels[:max_levels]:
            if level_amount <= 0:
                continue
            cum_size_next = cum_size + level_amount
            cum_cost_next = cum_cost + level_price * level_amount
            if cum_cost_next > max_quote:
                add_quote = max_quote - cum_cost
                if add_quote <= 0:
                    break
                level_amount = add_quote / level_price
                cum_size_next = cum_size + level_amount
                cum_cost_next = cum_cost + level_price * level_amount

            cum_size = cum_size_next
            cum_cost = cum_cost_next
            if cum_cost >= max_quote:
                break

        if cum_size <= 0:
            return Decimal("0"), Decimal("0"), Decimal("0")

        vwap = cum_cost / cum_size
        size, vwap = self._apply_balance_and_min_size(cum_size, vwap, side)
        return size, vwap, cum_cost

    def _compute_profit_chunk_size(self, dex_fair: Decimal, side: TradeType) -> Tuple[Decimal, Decimal, Decimal]:
        """Return (base_amount, vwap, net_quote_est) using post-fee edge floor."""
        book = self.get_order_book(self.config.connector_name, self.config.trading_pair)
        levels = self._zone_levels(book, side, dex_fair)
        if not levels:
            return Decimal("0"), Decimal("0"), Decimal("0")

        max_quote = self._max_quote_per_chunk()
        cum_size = Decimal("0")
        cum_cost = Decimal("0")
        best: Optional[Tuple[Decimal, Decimal, Decimal]] = None
        min_edge = self.config.min_edge_bps_after_fees / Decimal("10000")
        max_levels = max(1, int(self.config.max_walk_levels))
        consecutive_drops = 0

        for level_price, level_amount in levels[:max_levels]:
            if level_amount <= 0:
                continue
            cum_size_next = cum_size + level_amount
            cum_cost_next = cum_cost + level_price * level_amount
            if cum_cost_next > max_quote:
                add_quote = max_quote - cum_cost
                if add_quote <= 0:
                    break
                level_amount = add_quote / level_price
                cum_size_next = cum_size + level_amount
                cum_cost_next = cum_cost + level_price * level_amount

            cum_size = cum_size_next
            cum_cost = cum_cost_next
            vwap = cum_cost / cum_size if cum_size > 0 else Decimal("0")

            if side == TradeType.BUY:
                gross_per_unit = dex_fair - vwap
            else:
                gross_per_unit = vwap - dex_fair

            fee_quote = self._estimate_taker_fee_quote(side, cum_size, vwap)
            net_quote = gross_per_unit * cum_size - fee_quote
            if dex_fair <= 0 or cum_size <= 0:
                break
            net_edge_per_unit = net_quote / cum_size
            if net_quote <= 0 or net_edge_per_unit / dex_fair < min_edge:
                break

            if best is None or net_quote > best[2]:
                best = (cum_size, vwap, net_quote)
                consecutive_drops = 0
            else:
                consecutive_drops += 1
                if consecutive_drops >= 2:
                    break

            if cum_cost >= max_quote:
                break

        if best is None or best[2] <= 0:
            return Decimal("0"), Decimal("0"), Decimal("0")

        size, vwap, net_est = best
        size, vwap = self._apply_balance_and_min_size(size, vwap, side)
        return size, vwap, net_est

    def _compute_chunk_size(self, dex_fair: Decimal, side: TradeType) -> Tuple[Decimal, Decimal, Decimal]:
        if self.config.rebalance_only:
            return self._compute_rebalance_chunk_size(dex_fair, side)
        return self._compute_profit_chunk_size(dex_fair, side)

    def _compute_zone_quote(self, dex_fair: Decimal, side: TradeType) -> Decimal:
        """Return total quote amount available in the convergence zone."""
        book = self.get_order_book(self.config.connector_name, self.config.trading_pair)
        levels = self._zone_levels(book, side, dex_fair)
        max_levels = max(1, int(self.config.max_walk_levels))
        zone_quote = Decimal("0")
        for level_price, level_amount in levels[:max_levels]:
            zone_quote += level_price * level_amount
        return zone_quote

    def _side_allowed(self, side: TradeType) -> bool:
        if self.config.allowed_sides == "both":
            return True
        if self.config.allowed_sides == "buy":
            return side == TradeType.BUY
        return side == TradeType.SELL

    def _pick_side(self, cex_mid: Decimal, dex_fair: Decimal) -> Optional[TradeType]:
        self._update_signed_gap_bps(cex_mid, dex_fair)
        if abs(self._last_gap_bps) < self.config.min_gap_bps:
            self._skip_reason = f"gap {self._last_gap_bps:.1f}bps < min_gap_bps {self.config.min_gap_bps}"
            return None
        if cex_mid > dex_fair:
            side = TradeType.SELL
        else:
            side = TradeType.BUY
        if not self._side_allowed(side):
            self._skip_reason = f"side {side} not allowed ({self.config.allowed_sides})"
            return None
        return side

    def _gap_closed(self, cex_mid: Decimal, dex_fair: Decimal) -> bool:
        gap_bps = abs((cex_mid - dex_fair) / dex_fair * Decimal("10000"))
        return gap_bps < self.config.min_gap_bps

    def _set_sweep_side(self, side: TradeType) -> None:
        self._sweep_side = side
        self.config.side = side

    def _apply_exit_close_type(self) -> None:
        """H-2: hold inventory when any chunk filled; else completed with no trade."""
        if self._total_filled_base > 0:
            self.close_type = CloseType.POSITION_HOLD
        else:
            self.close_type = CloseType.COMPLETED

    def _append_held_order_if_new(self, tracked: TrackedOrder) -> None:
        if not tracked or not tracked.order:
            return
        oid = tracked.order.client_order_id
        if any(
            isinstance(h, dict) and h.get("client_order_id") == oid
            for h in self._held_position_orders
        ):
            return
        self._held_position_orders.append(tracked.order.to_json())

    # ------------------------------------------------------------------ control
    async def validate_sufficient_balance(self):
        # H-5: only verify feed can start; gap/side/size deferred to control loop (EARLY_STOP).
        if not self._ensure_dex_feed():
            self.close_type = CloseType.FAILED
            self.stop()

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            await self._running_step()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self._shutdown_step()
        self._evaluate_max_retries()

    async def _running_step(self):
        if self._sweep_complete:
            return
        if self._order is not None and self._order.order and not self._order.is_done:
            if self.config.debug_verbose:
                self.logger().debug(
                    "Convergence [%s] tick order_pending chunks_left=%s",
                    self._controller_id(),
                    self._chunks_remaining,
                )
            return

        if self.config.debug_verbose:
            self.logger().debug(
                "Convergence [%s] tick sweep_complete=%s chunks_left=%s sweep_side=%s",
                self._controller_id(),
                self._sweep_complete,
                self._chunks_remaining,
                self._sweep_side,
            )

        if not self._ensure_dex_feed():
            self._log_wait("dex_feed_unavailable", reason=self._skip_reason or "unknown")
            return

        cex_mid = self._cex_mid()
        if cex_mid is None:
            self._log_wait("cex_mid_unavailable", reason=self._skip_reason or "unknown")
            return

        await self._poll_dex_async(cex_mid)
        self._last_cex_mid = cex_mid

        if self._dex_feed.is_stale():
            self._skip_reason = "dex feed stale"
            self._log_wait("dex_feed_stale", cex_mid=cex_mid)
            return
        dex_fair = self._dex_feed.get_dex_fair()
        if dex_fair is None:
            self._skip_reason = "dex_fair unavailable"
            self._log_wait("dex_fair_unavailable", cex_mid=cex_mid)
            return
        if not self._dex_feed.sanity_ok(cex_mid):
            self._skip_reason = "dex sanity check failed"
            self._log_wait("dex_sanity_failed", cex_mid=cex_mid, dex_fair=dex_fair)
            return

        self._last_dex_fair = dex_fair

        if self._sweep_side is None:
            side = self._pick_side(cex_mid, dex_fair)
            if side is None:
                self._log_skip_finish(
                    self._skip_reason or "pick_side_failed",
                    cex_mid=cex_mid,
                    dex_fair=dex_fair,
                )
                self._sweep_complete = True
                self.close_type = CloseType.EARLY_STOP
                self.stop()
                return
            self._set_sweep_side(side)
            self._chunks_remaining = max(1, int(self.config.sweep_chunks))
            self._init_widening_ref(cex_mid, dex_fair)
            self._log_decision(
                "sweep_started",
                side=side,
                gap_bps=self._last_gap_bps,
                widening_ref_bps=self._widening_ref_bps,
                cex_mid=cex_mid,
                dex_fair=dex_fair,
                chunks=self._chunks_remaining,
            )
        else:
            if self._gap_closed(cex_mid, dex_fair):
                self._log_skip_finish(
                    "gap_closed_mid_sweep",
                    cex_mid=cex_mid,
                    dex_fair=dex_fair,
                    side=self._sweep_side,
                )
                self._finish_sweep()
                return
            side = self._sweep_side

        if self._chunks_remaining <= 0:
            self._log_skip_finish(
                "chunks_exhausted",
                cex_mid=cex_mid,
                dex_fair=dex_fair,
                side=side,
            )
            self._finish_sweep()
            return

        zone_quote = self._compute_zone_quote(dex_fair, side)
        if zone_quote >= self.config.zone_quote_threshold_usdt:
            self._skip_reason = (
                f"zone quote {zone_quote:.6f} >= threshold {self.config.zone_quote_threshold_usdt}"
            )
            self._log_skip_finish(
                "zone_quote_above_threshold",
                cex_mid=cex_mid,
                dex_fair=dex_fair,
                side=side,
                zone_quote=zone_quote,
                threshold=self.config.zone_quote_threshold_usdt,
            )
            self._finish_sweep()
            return

        size, vwap, quote_est = self._compute_chunk_size(dex_fair, side)
        if size <= 0:
            self._skip_reason = (
                "no size available in zone for chunk"
                if self.config.rebalance_only
                else "no size clears edge floor for chunk"
            )
            self._log_skip_finish(
                "chunk_size_zero",
                cex_mid=cex_mid,
                dex_fair=dex_fair,
                side=side,
                zone_quote=zone_quote,
                rebalance_only=self.config.rebalance_only,
            )
            self._finish_sweep()
            return

        self._update_signed_gap_bps(cex_mid, dex_fair)
        current_abs_gap = self._abs_gap_bps(cex_mid, dex_fair)
        if not self._gap_widening_allows_trade(cex_mid, dex_fair):
            self._skip_reason = (
                f"gap not widening: {current_abs_gap:.1f}bps <= ref {self._widening_ref_bps:.1f}bps"
            )
            self._log_wait(
                "gap_not_widening",
                current_abs_gap=current_abs_gap,
                widening_ref_bps=self._widening_ref_bps,
                signed_gap_bps=self._last_gap_bps,
                cex_mid=cex_mid,
                dex_fair=dex_fair,
            )
            return

        self._abs_gap_at_place = current_abs_gap
        est_label = "zone_quote_est" if self.config.rebalance_only else "est_net_quote"
        self._log_decision(
            "placing_market",
            side=side,
            size=size,
            vwap=vwap,
            dex_fair=dex_fair,
            cex_mid=cex_mid,
            gap_bps=self._last_gap_bps,
            abs_gap_bps=current_abs_gap,
            widening_ref_bps=self._widening_ref_bps,
            **{est_label: quote_est},
            chunks_left=self._chunks_remaining,
        )
        self._place_market_order(side, size)

    def _place_market_order(self, side: TradeType, amount: Decimal):
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=OrderType.MARKET,
            side=side,
            amount=amount,
            price=Decimal("NaN"),
        )
        self._order = TrackedOrder(order_id=order_id)

    def _finish_sweep(self):
        self._sweep_complete = True
        self._apply_exit_close_type()
        self._status = RunnableStatus.SHUTTING_DOWN

    def early_stop(self, keep_position: bool = False, skip_order_cancel: bool = False):
        """C-1: orchestrator / StopExecutorAction must not hit NotImplementedError."""
        if (
            not skip_order_cancel
            and self._order
            and self._order.order
            and self._order.order.is_open
        ):
            self._strategy.cancel(
                self.config.connector_name,
                self.config.trading_pair,
                self._order.order_id,
            )
        self._sweep_complete = True
        if keep_position or self._total_filled_base > 0:
            self.close_type = CloseType.POSITION_HOLD
        else:
            self.close_type = CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN

    async def _shutdown_step(self):
        if self._order and self._order.order and self._order.is_open:
            self._strategy.cancel(
                self.config.connector_name,
                self.config.trading_pair,
                self._order.order_id,
            )
            await asyncio.sleep(0.5)
        elif self._order and self._order.is_filled:
            self._append_held_order_if_new(self._order)
            self._apply_exit_close_type()
            self.stop()
        elif self._order is None or self._order.is_done:
            self._apply_exit_close_type()
            self.stop()

    def _evaluate_max_retries(self):
        if self._current_retries >= self._max_retries:
            self.close_type = CloseType.FAILED
            self.stop()

    # ------------------------------------------------------------------ events
    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if self._order and self._order.order_id == event.order_id:
            self._order.order = self.get_in_flight_order(self.config.connector_name, event.order_id)

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        if self._order and self._order.order_id == event.order_id:
            self._order.order = self.get_in_flight_order(self.config.connector_name, event.order_id)

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        if self._order and self._order.order_id == event.order_id:
            self._order.order = self.get_in_flight_order(self.config.connector_name, event.order_id)
            if self._order.order:
                self._total_filled_base += self._order.order.executed_amount_base
                self._total_filled_quote += self._chunk_fill_quote(event, self._order)
                self._cum_fees_quote += self._order.cum_fees_quote
                self._append_held_order_if_new(self._order)
            chunk_filled_base = (
                self._order.order.executed_amount_base
                if self._order and self._order.order
                else Decimal("0")
            )
            self._order = None
            self._chunks_remaining -= 1
            if self.config.require_gap_widening and self._abs_gap_at_place is not None:
                self._widening_ref_bps = self._abs_gap_at_place
            self._log_decision(
                "chunk_complete",
                filled_base=chunk_filled_base,
                chunks_left=self._chunks_remaining,
                total_filled_base=self._total_filled_base,
                widening_ref_bps=self._widening_ref_bps,
            )
            if self._chunks_remaining <= 0 or self._sweep_complete:
                self._apply_exit_close_type()
                self._status = RunnableStatus.SHUTTING_DOWN
            # else: next chunk on next control tick

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if self._order and self._order.order_id == event.order_id:
            self._order = None
            self._current_retries += 1
            self.logger().error("Convergence order failed %s", event.order_id)

    # ------------------------------------------------------------------ pnl / status
    def get_net_pnl_quote(self) -> Decimal:
        if self._last_dex_fair is None:
            return Decimal("0")
        if not self._is_positive_finite(self._total_filled_base):
            return Decimal("0")
        if not self._is_positive_finite(self._total_filled_quote):
            return Decimal("0")
        avg_price = self._total_filled_quote / self._total_filled_base
        if self._sweep_side == TradeType.BUY:
            mark_pnl = (self._last_dex_fair - avg_price) * self._total_filled_base
        elif self._sweep_side == TradeType.SELL:
            mark_pnl = (avg_price - self._last_dex_fair) * self._total_filled_base
        else:
            mark_pnl = Decimal("0")
        return mark_pnl - self.get_cum_fees_quote()

    def get_net_pnl_pct(self) -> Decimal:
        if not self._is_positive_finite(self._total_filled_quote):
            return Decimal("0")
        return self.get_net_pnl_quote() / self._total_filled_quote

    def get_cum_fees_quote(self) -> Decimal:
        in_flight = Decimal("0")
        if self._order and self._order.order:
            in_flight = self._order.cum_fees_quote
        return self._cum_fees_quote + in_flight

    @property
    def filled_amount_quote(self) -> Decimal:
        return self._sanitize_quote_amount(self._total_filled_quote)

    def get_custom_info(self) -> Dict:
        return {
            "connector": self.config.connector_name,
            "trading_pair": self.config.trading_pair,
            "side": self.config.side or self._sweep_side,
            "dex_fair": self._last_dex_fair,
            "cex_mid": self._last_cex_mid,
            "gap_bps": self._last_gap_bps,
            "sweep_side": str(self._sweep_side) if self._sweep_side else None,
            "chunks_remaining": self._chunks_remaining,
            "zone_quote_threshold_usdt": self.config.zone_quote_threshold_usdt,
            "rebalance_only": self.config.rebalance_only,
            "total_filled_base": self._total_filled_base,
            "total_filled_quote": self._total_filled_quote,
            "cum_fees_quote": self._cum_fees_quote,
            "skip_reason": self._skip_reason,
            "require_gap_widening": self.config.require_gap_widening,
            "widening_ref_bps": self._widening_ref_bps,
            "spawn_gap_bps": self.config.spawn_gap_bps,
            "held_position_orders": self._held_position_orders,
        }

    def to_format_status(self):
        lines = [
            f"ConvergenceExecutor | {self.status} | close={self.close_type}",
            f"  pair={self.config.trading_pair} side={self._sweep_side} "
            f"chunks_left={self._chunks_remaining}",
            f"  dex_fair={self._last_dex_fair} cex_mid={self._last_cex_mid} gap_bps={self._last_gap_bps}",
        ]
        if self._skip_reason:
            lines.append(f"  skip: {self._skip_reason}")
        return lines

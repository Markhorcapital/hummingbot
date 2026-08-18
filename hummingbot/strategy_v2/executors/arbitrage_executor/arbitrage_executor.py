import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Union

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketOrderFailureEvent, SellOrderCreatedEvent
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class ArbitrageExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @staticmethod
    def _are_tokens_interchangeable(first_token: str, second_token: str):
        interchangeable_tokens = [
            {"WETH", "ETH"},
            {"WBTC", "BTC"},
            {"WBNB", "BNB"},
            {"WPOL", "POL"},
            {"WAVAX", "AVAX"},
            {"WONE", "ONE"},
        ]
        same_token_condition = first_token == second_token
        tokens_interchangeable_condition = any(({first_token, second_token} <= interchangeable_pair
                                                for interchangeable_pair
                                                in interchangeable_tokens))
        # for now, we will consider all the stablecoins interchangeable
        stable_coins_condition = "USD" in first_token and "USD" in second_token
        return same_token_condition or tokens_interchangeable_condition or stable_coins_condition

    def __init__(self,
                 strategy: ScriptStrategyBase,
                 config: ArbitrageExecutorConfig,
                 update_interval: float = 1.0,
                 max_retries: int = 3):
        if not self.is_arbitrage_valid(pair1=config.buying_market.trading_pair,
                                       pair2=config.selling_market.trading_pair):
            raise Exception("Arbitrage is not valid since the trading pairs are not interchangeable.")
        super().__init__(strategy=strategy,
                         connectors=[config.buying_market.connector_name, config.selling_market.connector_name],
                         config=config, update_interval=update_interval)
        self.config = config
        self.buying_market = config.buying_market
        self.selling_market = config.selling_market
        self.min_profitability = config.min_profitability
        self.order_amount = config.order_amount
        self.max_retries = max_retries

        # Order tracking
        self._buy_order: TrackedOrder = TrackedOrder()
        self._sell_order: TrackedOrder = TrackedOrder()

        self._last_buy_price = Decimal("1")
        self._last_sell_price = Decimal("1")
        self._trade_pnl_pct = Decimal("0")
        self._last_tx_cost = Decimal("0")
        self._last_buy_fee = Decimal("0")
        self._last_sell_fee = Decimal("0")
        self._current_profitability = Decimal("0")
        self._amm_gas_amount = Decimal("0")
        self._amm_gas_cost = Decimal("0")

        # --- auto-sizing state ----------------------------------------------
        # _optimal_size is the size the latest walk chose. When auto_size is
        # False this stays equal to the fixed order_amount.
        self._optimal_size: Decimal = self.order_amount
        # Slippage coefficient k cached between walks for the analytic gate.
        self._k_cached: Optional[Decimal] = None
        self._k_cached_ts: float = 0.0
        # dex_spot recorded at the start of the most recent walk; used by the
        # pre-execution drift check.
        self._last_dex_spot_at_walk: Optional[Decimal] = None
        # Last computed net profit estimate for the optimal size, for logging.
        self._last_net_profit_quote: Decimal = Decimal("0")
        self._auto_size_skip_reason: Optional[str] = None

        # Quote asset conversion rate
        _, buy_quote_asset = split_hb_trading_pair(self.buying_market.trading_pair)
        _, sell_quote_asset = split_hb_trading_pair(self.selling_market.trading_pair)

        # Define the conversion trading pair (e.g., SOL/USDT or USDT/SOL)
        self.quote_conversion_pair = f"{sell_quote_asset}-{buy_quote_asset}"
        self.rate_oracle = RateOracle.get_instance()
        self._cumulative_failures = 0

    # ------------------------------------------------------------------ utils
    @property
    def _auto_size_enabled(self) -> bool:
        """Strict-bool check on the auto_size config flag.

        Defensive against duck-typed truthy values that aren't explicit
        booleans (the field is declared as `bool` in the pydantic config,
        but accessing through `getattr` lets us fail closed on misuse).
        """
        return getattr(self.config, "auto_size", False) is True

    def _size_band(self) -> Tuple[Decimal, Decimal]:
        """Return (min_size, max_size) for the auto-sizing walk."""
        min_amt = self.config.min_order_amount if self.config.min_order_amount is not None else self.order_amount
        max_amt = self.config.max_order_amount if self.config.max_order_amount is not None else self.order_amount
        return Decimal(str(min_amt)), Decimal(str(max_amt))

    def _probe_amount(self) -> Decimal:
        """Tiny amount used to read live VWAPs as a proxy for spot."""
        if self.config.tiny_probe_amount is not None:
            return Decimal(str(self.config.tiny_probe_amount))
        min_amt, _ = self._size_band()
        return min_amt

    async def validate_sufficient_balance(self):
        # When auto-sizing we only require the floor amount up-front. The
        # per-tick walk will independently clamp the size to live balance.
        check_amount = self._size_band()[0] if self._auto_size_enabled else self.order_amount

        base_asset_for_selling_exchange = self.connectors[self.selling_market.connector_name].get_available_balance(
            self.selling_market.trading_pair.split("-")[0])
        if check_amount > base_asset_for_selling_exchange:
            self.logger().info(f"Insufficient balance in exchange {self.selling_market.connector_name} "
                               f"to sell {self.selling_market.trading_pair.split('-')[0]} "
                               f"Actual: {base_asset_for_selling_exchange} --> Needed: {check_amount}")
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough budget to open position.")
            self.stop()
            return

        price = await self.get_resulting_price_for_amount(
            exchange=self.buying_market.connector_name,
            trading_pair=self.buying_market.trading_pair,
            is_buy=True,
            order_amount=check_amount)
        quote_asset_for_buying_exchange = self.connectors[self.buying_market.connector_name].get_available_balance(
            self.buying_market.trading_pair.split("-")[1])
        if check_amount * price > quote_asset_for_buying_exchange:
            self.logger().info(f"Insufficient balance in exchange {self.buying_market.connector_name} "
                               f"to buy {self.buying_market.trading_pair.split('-')[1]} "
                               f"Actual: {quote_asset_for_buying_exchange} --> Needed: {check_amount * price}")
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough budget to open position.")
            self.stop()
            return

    def is_arbitrage_valid(self, pair1, pair2):
        base_asset1, quote_asset1 = split_hb_trading_pair(pair1)
        base_asset2, quote_asset2 = split_hb_trading_pair(pair2)
        return self._are_tokens_interchangeable(base_asset1, base_asset2)

    def get_net_pnl_quote(self) -> Decimal:
        if self.close_type == CloseType.COMPLETED:
            sell_quote_amount = self.sell_order.order.executed_amount_base * self.sell_order.average_executed_price
            buy_quote_amount = self.buy_order.order.executed_amount_base * self.buy_order.average_executed_price
            return sell_quote_amount - buy_quote_amount - self.cum_fees_quote
        else:
            return Decimal("0")

    def get_net_pnl_pct(self) -> Decimal:
        if self.is_closed:
            if self.buy_order.order and self.buy_order.order.executed_amount_base > 0:
                return self.net_pnl_quote / self.buy_order.order.executed_amount_base
            else:
                return Decimal("0")
        else:
            return Decimal("0")

    def get_cum_fees_quote(self) -> Decimal:
        return self.buy_order.cum_fees_quote + self.sell_order.cum_fees_quote

    @property
    def buy_order(self) -> TrackedOrder:
        return self._buy_order

    @buy_order.setter
    def buy_order(self, value: TrackedOrder):
        self._buy_order = value

    @property
    def sell_order(self) -> TrackedOrder:
        return self._sell_order

    @sell_order.setter
    def sell_order(self, value: TrackedOrder):
        self._sell_order = value

    async def get_resulting_price_for_amount(self, exchange: str, trading_pair: str, is_buy: bool,
                                             order_amount: Decimal):
        return await self.connectors[exchange].get_quote_price(trading_pair, is_buy, order_amount)

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            try:
                if self._auto_size_enabled:
                    await self._auto_size_control_step()
                else:
                    await self._fixed_size_control_step()
            except Exception as e:
                self.logger().error(f"Error calculating profitability: {e}")
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            if self._cumulative_failures > self.max_retries:
                self.close_type = CloseType.FAILED
                self.stop()
            else:
                self.check_order_status()

    async def _fixed_size_control_step(self):
        """Original behaviour: fixed-amount profitability check."""
        await self.update_trade_pnl_pct()
        await self.update_tx_cost()
        self._current_profitability = (
            self._trade_pnl_pct * self.order_amount - self._last_tx_cost) / self.order_amount
        if self._current_profitability > self.min_profitability:
            await self.execute_arbitrage()

    # ------------------------------------------------------------- auto-size
    async def _auto_size_control_step(self):
        """Hybrid Option 1 + Option 3 control step (see data_types docstring)."""
        self._auto_size_skip_reason = None

        # 1. Sample live prices ------------------------------------------------
        probe = self._probe_amount()
        try:
            buy_probe_price, sell_probe_price = await self._get_prices_for_amount(probe)
        except Exception as e:
            self._auto_size_skip_reason = f"probe failed: {e}"
            return
        if not buy_probe_price or not sell_probe_price:
            self._auto_size_skip_reason = "probe returned no price"
            return

        try:
            conversion_rate = await self.get_quote_asset_conversion_rate()
        except Exception as e:
            self._auto_size_skip_reason = f"conversion rate failed: {e}"
            return
        live_basis = (sell_probe_price * conversion_rate - buy_probe_price) / buy_probe_price
        # Cache the dex spot used by the walk for the drift check at the end.
        self._last_dex_spot_at_walk = sell_probe_price if self._selling_is_amm() else buy_probe_price

        # 2. Analytic gate (Option 3) -----------------------------------------
        if not self._analytic_gate(live_basis):
            self._auto_size_skip_reason = (
                f"gate: basis={live_basis * Decimal('10000'):.1f}bps below threshold")
            return

        # 3. Walk (Option 1) --------------------------------------------------
        walk_result = await self._compute_optimal_size(buy_probe_price, sell_probe_price, conversion_rate)
        if walk_result is None:
            self._auto_size_skip_reason = "walk yielded no profitable size"
            return
        size, net_profit, buy_vwap, sell_vwap, walk_samples = walk_result

        # 4. Constraints ------------------------------------------------------
        if not self._apply_constraints(size, net_profit, buy_vwap):
            return

        # 5. Drift check (spot + size-level VWAP) -----------------------------
        # The DEX leg's expected VWAP is sell_vwap when we sell on DEX, else
        # buy_vwap (we'd be buying on DEX).
        expected_dex_vwap = sell_vwap if self._selling_is_amm() else buy_vwap
        if not await self._drift_check(size=size, expected_dex_vwap=expected_dex_vwap):
            return

        # 6. Commit to executing this size and price --------------------------
        self._optimal_size = size
        self._last_buy_price = buy_vwap
        self._last_sell_price = sell_vwap
        self._last_net_profit_quote = net_profit
        normalized_sell = sell_vwap * conversion_rate
        self._trade_pnl_pct = (normalized_sell - buy_vwap) / buy_vwap
        self._current_profitability = self._last_net_profit_quote / (size * buy_vwap) if size * buy_vwap > 0 else Decimal("0")
        self._refresh_k_cache(walk_samples)
        await self.execute_arbitrage()

    def _selling_is_amm(self) -> bool:
        return self.is_amm_connector(self.selling_market.connector_name)

    async def _get_prices_for_amount(self, amount: Decimal) -> Tuple[Decimal, Decimal]:
        buy_task = asyncio.create_task(self.get_resulting_price_for_amount(
            exchange=self.buying_market.connector_name,
            trading_pair=self.buying_market.trading_pair,
            is_buy=True,
            order_amount=amount))
        sell_task = asyncio.create_task(self.get_resulting_price_for_amount(
            exchange=self.selling_market.connector_name,
            trading_pair=self.selling_market.trading_pair,
            is_buy=False,
            order_amount=amount))
        return await asyncio.gather(buy_task, sell_task)

    def _analytic_gate(self, live_basis: Decimal) -> bool:
        """Cheap pre-filter using the cached slippage coefficient.

        We model net basis as b(s) ≈ b0 − k·s. Peak profit (per quote unit
        deployed) is (b0 − fees)² / (4k), minus fixed gas. We use this purely
        as a skip filter; the walk is the source of truth.
        """
        min_basis = Decimal(str(self.config.min_basis_bps)) / Decimal("10000")
        if abs(live_basis) < min_basis:
            return False

        # If we don't yet have a calibrated k, always let the walk run so we
        # can collect data points to estimate k.
        if self._k_cached is None or time.time() - self._k_cached_ts > self.config.k_cache_ttl:
            return True
        if self._k_cached <= 0:
            return True

        # Convert min_profitability (bps as Decimal) to the same unit as basis.
        fees_bps = self.min_profitability  # already a fractional rate
        adjusted_basis = abs(live_basis) - fees_bps
        if adjusted_basis <= 0:
            return False
        # Peak size (in base units) that maximises the parabola.
        peak_size = adjusted_basis / (Decimal("2") * self._k_cached)
        if peak_size <= 0:
            return False
        # Cap by max size for the comparison.
        _, max_size = self._size_band()
        peak_size = min(peak_size, max_size)
        # Peak net profit in quote currency (very rough — gas is approximated as last_tx_cost).
        peak_profit = peak_size * (adjusted_basis - self._k_cached * peak_size) - self._last_tx_cost
        return peak_profit >= self.config.min_net_profit_quote

    async def _compute_optimal_size(self,
                                    initial_buy_price: Decimal,
                                    initial_sell_price: Decimal,
                                    conversion_rate: Decimal,
                                    ) -> Optional[Tuple[Decimal, Decimal, Decimal, Decimal, List[Tuple[Decimal, Decimal]]]]:
        """Walk the CEX order book and re-quote DEX VWAP at each step.

        Returns (size, net_profit_quote, buy_vwap, sell_vwap, walk_samples)
        where walk_samples is a list of (size, gross_basis) used for k
        estimation. Returns None for any of:
          - cex order book is unavailable
          - profitable zone is empty for the live probe prices
          - a DEX quote returns None mid-walk before clearing min_size
          - no level past min_size yielded a positive net_quote
        Net profit is computed as `gross_quote − tx_cost`, where `tx_cost`
        already includes both legs' costs (AMM gas + CEX taker fee, per
        update_tx_cost). The previous version double-subtracted the CEX fee
        by also applying `min_profitability × notional` on top of tx_cost;
        that has been removed. `min_profitability` is now used only as a bps
        floor by the analytic gate (Option 3), not in the walk's net math.
        """
        min_size, max_size = self._size_band()
        cex_connector_name, dex_connector_name, cex_pair, dex_pair, cex_is_buy = self._cex_dex_split()
        cex_book = self._safe_get_order_book(cex_connector_name, cex_pair)
        if cex_book is None:
            return None

        levels = self._cex_zone_levels(cex_book, cex_is_buy, initial_buy_price, initial_sell_price)
        if not levels:
            return None

        # tx_cost includes BOTH legs' realised cost (AMM gas + CEX fee). It is
        # measured once per walk at min_size. AMM gas in token units is
        # roughly fixed per swap; the CEX fee component scales with size but
        # the size-weighted error over the walk is small relative to the
        # additional RPC cost of remeasuring at each step.
        await self._update_tx_cost_for_amount(min_size)
        tx_cost = self._last_tx_cost

        cum_size = Decimal("0")
        cum_cost = Decimal("0")
        best: Optional[Tuple[Decimal, Decimal, Decimal, Decimal]] = None
        samples: List[Tuple[Decimal, Decimal]] = []
        consecutive_drops = 0
        max_levels = max(1, int(self.config.max_walk_levels))

        for _, (level_price, level_amount) in enumerate(levels[:max_levels]):
            level_price = Decimal(str(level_price))
            level_amount = Decimal(str(level_amount))
            if level_amount <= 0:
                continue
            cum_size_next = cum_size + level_amount
            # Clamp inline to max_size.
            if cum_size_next > max_size:
                level_amount = max_size - cum_size
                if level_amount <= 0:
                    break
                cum_size_next = max_size
            cum_cost_next = cum_cost + level_price * level_amount
            cum_size = cum_size_next
            cum_cost = cum_cost_next
            cex_vwap = cum_cost / cum_size

            try:
                dex_vwap = await self.get_resulting_price_for_amount(
                    exchange=dex_connector_name,
                    trading_pair=dex_pair,
                    is_buy=not cex_is_buy,
                    order_amount=cum_size)
            except Exception:
                dex_vwap = None
            if not dex_vwap:
                break
            dex_vwap = Decimal(str(dex_vwap))

            buy_vwap, sell_vwap = self._assign_buy_sell(cex_vwap, dex_vwap, cex_is_buy)
            normalized_sell = sell_vwap * conversion_rate
            gross_basis = (normalized_sell - buy_vwap) / buy_vwap if buy_vwap > 0 else Decimal("0")
            gross_quote = (normalized_sell - buy_vwap) * cum_size
            # tx_cost already covers both legs' costs; do NOT also subtract
            # min_profitability * notional (that was the bug from H-2).
            net_quote = gross_quote - tx_cost

            samples.append((cum_size, gross_basis))

            if cum_size < min_size:
                # Always keep walking until we clear the floor.
                continue

            if best is None or net_quote > best[1]:
                best = (cum_size, net_quote, buy_vwap, sell_vwap)
                consecutive_drops = 0
            else:
                consecutive_drops += 1
                # Two strictly-non-improving steps past the peak is a robust
                # stop on lumpy V3 tick boundaries (single dips can be local).
                if consecutive_drops >= 2:
                    break

            if cum_size >= max_size:
                break

        if best is None:
            return None
        size, net_profit, buy_vwap, sell_vwap = best
        return size, net_profit, buy_vwap, sell_vwap, samples

    def _cex_dex_split(self) -> Tuple[str, str, str, str, bool]:
        """Identify which leg is CEX and which is DEX, and the cex trade side.

        Returns (cex_connector, dex_connector, cex_pair, dex_pair, cex_is_buy).
        cex_is_buy is True iff we *buy* on CEX (and therefore sell on DEX).
        """
        buying_amm = self.is_amm_connector(self.buying_market.connector_name)
        selling_amm = self.is_amm_connector(self.selling_market.connector_name)
        if buying_amm and not selling_amm:
            # buy on DEX, sell on CEX
            return (self.selling_market.connector_name, self.buying_market.connector_name,
                    self.selling_market.trading_pair, self.buying_market.trading_pair, False)
        if selling_amm and not buying_amm:
            # buy on CEX, sell on DEX
            return (self.buying_market.connector_name, self.selling_market.connector_name,
                    self.buying_market.trading_pair, self.selling_market.trading_pair, True)
        # CEX↔CEX arb: treat buying_market as the "cex" side we walk and
        # selling_market as the venue we re-quote against.
        return (self.buying_market.connector_name, self.selling_market.connector_name,
                self.buying_market.trading_pair, self.selling_market.trading_pair, True)

    def _safe_get_order_book(self, connector_name: str, trading_pair: str):
        try:
            return self.connectors[connector_name].get_order_book(trading_pair)
        except Exception as e:
            self.logger().warning(f"Could not get order book for {connector_name} {trading_pair}: {e}")
            return None

    @staticmethod
    def _cex_zone_levels(order_book, cex_is_buy: bool, buy_probe: Decimal, sell_probe: Decimal
                         ) -> List[Tuple[Decimal, Decimal]]:
        """Return CEX book levels in the profitable zone vs. the opposite venue probe.

        If we BUY on CEX, the zone is the asks that are CHEAPER than the
        venue we're selling to. If we SELL on CEX, the zone is the bids
        that are MORE EXPENSIVE than the venue we're buying from.
        """
        levels: List[Tuple[Decimal, Decimal]] = []
        if cex_is_buy:
            # Buy on CEX → cross asks. Profitable if ask < sell-venue probe.
            threshold = Decimal(str(sell_probe))
            for row in order_book.ask_entries():
                price = Decimal(str(row.price))
                if price >= threshold:
                    break
                levels.append((price, Decimal(str(row.amount))))
        else:
            # Sell on CEX → cross bids. Profitable if bid > buy-venue probe.
            threshold = Decimal(str(buy_probe))
            for row in order_book.bid_entries():
                price = Decimal(str(row.price))
                if price <= threshold:
                    break
                levels.append((price, Decimal(str(row.amount))))
        return levels

    @staticmethod
    def _assign_buy_sell(cex_vwap: Decimal, dex_vwap: Decimal, cex_is_buy: bool) -> Tuple[Decimal, Decimal]:
        return (cex_vwap, dex_vwap) if cex_is_buy else (dex_vwap, cex_vwap)

    def _refresh_k_cache(self, samples: List[Tuple[Decimal, Decimal]]):
        """Estimate the slippage coefficient k from walk samples (Option 3 input).

        Uses ordinary least squares over all samples for `b(s) = b0 − k·s`,
        which is more robust than a two-point slope on lumpy V3 books.
        Invalidates the cache (sets `_k_cached = None`) when the fit is
        inconclusive (single distinct size, non-positive variance, or a
        non-positive slope), so a stale k from minutes ago never silently
        survives a non-monotone walk.
        """
        if len(samples) < 2:
            self._k_cached = None
            return
        # Filter to unique sizes; drop duplicates that contribute no variance.
        sizes = [Decimal(str(s)) for s, _ in samples]
        bases = [Decimal(str(b)) for _, b in samples]
        n = Decimal(len(sizes))
        sum_s = sum(sizes, Decimal("0"))
        sum_b = sum(bases, Decimal("0"))
        mean_s = sum_s / n
        mean_b = sum_b / n
        num = Decimal("0")
        den = Decimal("0")
        for s, b in zip(sizes, bases):
            ds = s - mean_s
            num += ds * (b - mean_b)
            den += ds * ds
        if den <= 0:
            # All sizes equal: cannot estimate slope.
            self._k_cached = None
            return
        slope = num / den
        k = -slope
        if k <= 0:
            # Basis grew with size — model assumption violated. Drop cache.
            self._k_cached = None
            return
        self._k_cached = k
        self._k_cached_ts = time.time()

    def _apply_constraints(self, size: Decimal, net_profit: Decimal, buy_vwap: Decimal) -> bool:
        """Reject the walk's chosen (size, net) against the configured limits.

        `buy_vwap` is the fresh per-tick buy-side VWAP from the walker — never
        `self._last_buy_price`, which still reflects the previous tick and
        defaults to 1 on the very first tick (which would silently bypass the
        buy-side balance reserve for any token priced > 1).
        """
        min_size, _ = self._size_band()
        if size < min_size:
            self._auto_size_skip_reason = f"size {size} < min_order_amount {min_size}"
            return False
        # NOTE: size > max_size is intentionally NOT checked here because
        # _compute_optimal_size clamps cum_size to max_size inline. Re-checking
        # would be unreachable. An assertion is kept instead for safety in case
        # the walker invariant ever changes.
        _, max_size = self._size_band()
        assert size <= max_size, (
            f"walker returned size {size} above max_order_amount {max_size}; "
            "this should be impossible given the inline clamp in _compute_optimal_size"
        )
        if net_profit < self.config.min_net_profit_quote:
            self._auto_size_skip_reason = (
                f"net_profit {net_profit:.4f} < min_net_profit_quote {self.config.min_net_profit_quote}")
            return False

        # Balance reserve.
        reserve = self.config.max_balance_pct
        base, _ = split_hb_trading_pair(self.selling_market.trading_pair)
        sell_base_balance = Decimal(str(self.connectors[self.selling_market.connector_name].get_available_balance(base)))
        if size > sell_base_balance * reserve:
            self._auto_size_skip_reason = (
                f"size {size} exceeds {reserve} × sell-side base balance {sell_base_balance}")
            return False
        _, buy_quote = split_hb_trading_pair(self.buying_market.trading_pair)
        buy_quote_balance = Decimal(str(self.connectors[self.buying_market.connector_name].get_available_balance(buy_quote)))
        if buy_vwap <= 0:
            self._auto_size_skip_reason = f"invalid buy_vwap {buy_vwap} during balance check"
            return False
        required_quote = size * buy_vwap
        if required_quote > buy_quote_balance * reserve:
            self._auto_size_skip_reason = (
                f"required quote {required_quote:.4f} exceeds {reserve} × buy-side quote balance {buy_quote_balance}")
            return False
        return True

    async def _drift_check(self, size: Optional[Decimal] = None,
                           expected_dex_vwap: Optional[Decimal] = None) -> bool:
        """Two-stage pre-execution drift check.

        Stage 1 (spot): re-probe DEX at `tiny_probe_amount` and confirm it has
        not moved more than `max_drift_bps` from the spot we recorded at the
        start of the walk. Catches gross DEX movement during the walk.

        Stage 2 (size, optional): when `size` and `expected_dex_vwap` are
        supplied, also re-query DEX VWAP at the chosen size and confirm it
        has not moved more than `max_slippage_pct` from what the walker
        priced in. This is the "real" execution-time check; Gateway's 5s TTL
        cache typically makes this free because the walk just queried it.
        Without on-chain limit price plumbing (see notes in
        `_expected_price_with_slippage`), this check is the primary defence
        against executing at materially worse prices than the walk chose.
        """
        if self._last_dex_spot_at_walk is None:
            return True
        cex_conn, dex_conn, _cex_pair, dex_pair, cex_is_buy = self._cex_dex_split()

        # Stage 1: spot drift.
        if self.config.max_drift_bps > 0:
            try:
                new_dex_spot = await self.get_resulting_price_for_amount(
                    exchange=dex_conn,
                    trading_pair=dex_pair,
                    is_buy=not cex_is_buy,
                    order_amount=self._probe_amount())
            except Exception as e:
                self._auto_size_skip_reason = f"drift probe failed: {e}"
                return False
            if not new_dex_spot:
                self._auto_size_skip_reason = "drift probe returned no price"
                return False
            new_dex_spot = Decimal(str(new_dex_spot))
            drift = abs(new_dex_spot - self._last_dex_spot_at_walk) / self._last_dex_spot_at_walk
            max_drift = Decimal(str(self.config.max_drift_bps)) / Decimal("10000")
            if drift > max_drift:
                self._auto_size_skip_reason = (
                    f"dex_spot drift {drift * Decimal('10000'):.1f}bps > {self.config.max_drift_bps}bps")
                return False

        # Stage 2: VWAP drift at the chosen size.
        if size is None or expected_dex_vwap is None or self.config.max_slippage_pct <= 0:
            return True
        try:
            recheck_vwap = await self.get_resulting_price_for_amount(
                exchange=dex_conn,
                trading_pair=dex_pair,
                is_buy=not cex_is_buy,
                order_amount=size)
        except Exception as e:
            self._auto_size_skip_reason = f"size-level drift probe failed: {e}"
            return False
        if not recheck_vwap:
            self._auto_size_skip_reason = "size-level drift probe returned no price"
            return False
        recheck_vwap = Decimal(str(recheck_vwap))
        if expected_dex_vwap <= 0:
            self._auto_size_skip_reason = f"invalid expected_dex_vwap {expected_dex_vwap}"
            return False
        vwap_drift = abs(recheck_vwap - expected_dex_vwap) / expected_dex_vwap
        max_slippage = Decimal(str(self.config.max_slippage_pct))
        if vwap_drift > max_slippage:
            self._auto_size_skip_reason = (
                f"dex_vwap@size drift {vwap_drift * Decimal('10000'):.1f}bps > "
                f"max_slippage_pct {max_slippage * Decimal('10000'):.1f}bps")
            return False
        return True

    async def _update_tx_cost_for_amount(self, amount: Decimal):
        """Drop-in for update_tx_cost that uses a passed-in amount instead of order_amount."""
        original = self.order_amount
        self.order_amount = amount
        try:
            await self.update_tx_cost()
        finally:
            self.order_amount = original

    # ------------------------------------------------------------- execution
    def early_stop(self, keep_position: bool = False, skip_order_cancel: bool = False):
        self.close_type = CloseType.EARLY_STOP
        self.stop()

    def check_order_status(self):
        if self.buy_order.order and self.buy_order.order.is_filled and \
                self.sell_order.order and self.sell_order.order.is_filled:
            self.close_type = CloseType.COMPLETED
            self.stop()

    async def execute_arbitrage(self):
        self._status = RunnableStatus.SHUTTING_DOWN
        self.place_buy_arbitrage_order()
        self.place_sell_arbitrage_order()

    def _expected_price_with_slippage(self, base_price: Decimal, is_buy: bool, is_amm: bool) -> Decimal:
        """Bias the *informational* price reported to the connector by the
        configured slippage budget.

        IMPORTANT: as of Gateway's current `execute_swap` schema, the price
        passed to `place_order` is **not** forwarded as `amountOutMin` /
        `amountInMax` on the swap call — Gateway's `_create_order` carries the
        TODO `add limit_price to Gateway execute-swap schema`. Therefore this
        method only biases the price the connector tracks locally; it does NOT
        provide on-chain revert protection. On-chain protection has to be
        configured via Gateway's per-connector slippage tolerance.

        We still apply the bias because the local price is what flows into
        `start_tracking_order`, fee/PnL calculations, and UI display, so it
        is informationally more useful than the un-adjusted VWAP.
        """
        if not is_amm or self.config.max_slippage_pct <= 0:
            return base_price
        slip = Decimal(str(self.config.max_slippage_pct))
        return base_price * (Decimal("1") + slip) if is_buy else base_price * (Decimal("1") - slip)

    def _arb_amount(self) -> Decimal:
        return self._optimal_size if self._auto_size_enabled else self.order_amount

    def place_buy_arbitrage_order(self):
        is_amm = self.is_amm_connector(self.buying_market.connector_name)
        price = self._expected_price_with_slippage(self._last_buy_price, is_buy=True, is_amm=is_amm)
        self.buy_order.order_id = self.place_order(
            connector_name=self.buying_market.connector_name,
            trading_pair=self.buying_market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.BUY,
            amount=self._arb_amount(),
            price=price,
        )

    def place_sell_arbitrage_order(self):
        is_amm = self.is_amm_connector(self.selling_market.connector_name)
        price = self._expected_price_with_slippage(self._last_sell_price, is_buy=False, is_amm=is_amm)
        self.sell_order.order_id = self.place_order(
            connector_name=self.selling_market.connector_name,
            trading_pair=self.selling_market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.SELL,
            amount=self._arb_amount(),
            price=price,
        )

    async def update_tx_cost(self):
        base, quote = split_hb_trading_pair(trading_pair=self.buying_market.trading_pair)
        # TODO: also due the fact that we don't have a good rate oracle source we have to use a fixed token
        base_without_wrapped = base[1:] if base.startswith("W") else base
        buy_fee = await self.get_tx_cost_in_asset(
            exchange=self.buying_market.connector_name,
            trading_pair=self.buying_market.trading_pair,
            is_buy=True,
            order_amount=self.order_amount,
            asset=base_without_wrapped
        )
        sell_fee = await self.get_tx_cost_in_asset(
            exchange=self.selling_market.connector_name,
            trading_pair=self.selling_market.trading_pair,
            is_buy=False,
            order_amount=self.order_amount,
            asset=base_without_wrapped)
        self._last_buy_fee = buy_fee
        self._last_sell_fee = sell_fee
        self._last_tx_cost = self._last_buy_fee + self._last_sell_fee

    async def get_buy_and_sell_prices(self):
        return await self._get_prices_for_amount(self.order_amount)

    async def update_trade_pnl_pct(self):
        self._last_buy_price, self._last_sell_price = await self.get_buy_and_sell_prices()

        if not self._last_buy_price or not self._last_sell_price:
            raise Exception("Could not get buy and sell prices")

        # Fetch the conversion rate between quote assets
        conversion_rate = await self.get_quote_asset_conversion_rate()

        # Normalize the sell price to the same quote asset as the buy price
        normalized_sell_price = self._last_sell_price * conversion_rate

        # Calculate the profitability (PnL percentage)
        self._trade_pnl_pct = (normalized_sell_price - self._last_buy_price) / self._last_buy_price

    async def get_quote_asset_conversion_rate(self) -> Decimal:
        """
        Fetch the conversion rate between the quote assets of the buying and selling markets.
        Example: For M3M3/USDT and M3M3/SOL, fetch the SOL/USDT rate.
        """
        # Fetch the conversion rate from the connector
        try:
            conversion_rate = self.rate_oracle.get_pair_rate(self.quote_conversion_pair)
            return conversion_rate
        except Exception as e:
            self.logger().error(f"Error fetching conversion rate for {self.quote_conversion_pair}: {e}")
            raise

    async def get_tx_cost_in_asset(self, exchange: str, trading_pair: str, is_buy: bool, order_amount: Decimal,
                                   asset: str):
        connector = self.connectors[exchange]
        price = await self.get_resulting_price_for_amount(exchange, trading_pair, is_buy, order_amount)
        if self.is_amm_connector(exchange=exchange):
            gas_cost = connector.network_transaction_fee
            return gas_cost.amount / self.config.gas_conversion_price
        else:
            fee = connector.get_fee(
                base_currency=asset,
                quote_currency=asset,
                order_type=OrderType.MARKET,
                order_side=TradeType.BUY if is_buy else TradeType.SELL,
                amount=order_amount,
                price=price,
                is_maker=False
            )
            return fee.fee_amount_in_token(
                trading_pair=trading_pair,
                price=price,
                order_amount=order_amount,
                token=asset,
                exchange=connector,
            )

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if self.buy_order.order_id == event.order_id:
            self.buy_order.order = self.get_in_flight_order(self.buying_market.connector_name, event.order_id)
            self.logger().info("Buy Order Created")
        elif self.sell_order.order_id == event.order_id:
            self.logger().info("Sell Order Created")
            self.sell_order.order = self.get_in_flight_order(self.selling_market.connector_name, event.order_id)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if self.buy_order.order_id == event.order_id:
            self.place_buy_arbitrage_order()
            self._cumulative_failures += 1
        elif self.sell_order.order_id == event.order_id:
            self.place_sell_arbitrage_order()
            self._cumulative_failures += 1

    def get_custom_info(self) -> Dict:
        # Use the amount actually being traded (optimal size in auto-size
        # mode, fixed amount otherwise) so tx_cost_pct reflects reality.
        cost_divisor = self._arb_amount() if self._arb_amount() else Decimal("0")
        info = {
            "buy_connector": self.buying_market.connector_name,
            "sell_connector": self.selling_market.connector_name,
            "buy_pair": self.buying_market.trading_pair,
            "sell_pair": self.selling_market.trading_pair,
            "buy_price": self._last_buy_price,
            "sell_price": self._last_sell_price,
            "trade_pnl_pct": self._trade_pnl_pct,
            "tx_cost_pct": self._last_tx_cost / cost_divisor if cost_divisor else Decimal("0"),
            "profit_pct": self._current_profitability,
            "failures": self._cumulative_failures,
        }
        if self._auto_size_enabled:
            info.update({
                "auto_size": True,
                "optimal_size": self._optimal_size,
                "last_net_profit_quote": self._last_net_profit_quote,
                "k_cached": self._k_cached,
                "skip_reason": self._auto_size_skip_reason,
            })
        return info

    def to_format_status(self):
        lines = []
        if self._last_buy_price and self._last_sell_price:
            trade_pnl_pct = (self._last_sell_price - self._last_buy_price) / self._last_buy_price
            divisor = self._arb_amount() if self._arb_amount() else Decimal("1")
            tx_cost_pct = self._last_tx_cost / divisor
            base, quote = split_hb_trading_pair(trading_pair=self.buying_market.trading_pair)
            size_suffix = f" | Auto-size: {self._optimal_size}" if self._auto_size_enabled else ""
            lines.extend([f"""
    Arbitrage Status: {self.status} | Close Type: {self.close_type}
    - BUY: {self.buying_market.connector_name}:{self.buying_market.trading_pair}  --> SELL: {self.selling_market.connector_name}:{self.selling_market.trading_pair} | Amount: {self._arb_amount():.2f}{size_suffix}
    - Trade PnL (%): {trade_pnl_pct * 100:.2f} % | TX Cost (%): -{tx_cost_pct * 100:.2f} % | Net PnL (%): {(trade_pnl_pct - tx_cost_pct) * 100:.2f} %
    -------------------------------------------------------------------------------
    """])
            if self._auto_size_enabled and self._auto_size_skip_reason:
                lines.append(f"    Skipped this tick: {self._auto_size_skip_reason}\n")
            if self.close_type == CloseType.COMPLETED:
                lines.extend([f"Total Profit (%): {self.net_pnl_pct * 100:.2f} | Total Profit ({quote}): {self.net_pnl_quote:.4f}"])
            return lines
        else:
            msg = "There was an error while formatting the status for the executor."
            self.logger().warning(msg)
            lines.append(msg)
            return lines

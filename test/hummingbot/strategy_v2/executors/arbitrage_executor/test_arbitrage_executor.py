from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import MagicMock, Mock, PropertyMock, patch

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class TestArbitrageExecutor(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self):
        super().setUp()
        self.strategy = self.create_mock_strategy()
        self.arbitrage_config = MagicMock(spec=ArbitrageExecutorConfig)
        self.arbitrage_config.buying_market = ConnectorPair(connector_name='binance', trading_pair='POL-USDT')
        self.arbitrage_config.selling_market = ConnectorPair(connector_name='uniswap_polygon_mainnet', trading_pair='WPOL-USDT')
        self.arbitrage_config.min_profitability = Decimal('0.01')
        self.arbitrage_config.order_amount = Decimal('1')
        self.arbitrage_config.max_retries = 3
        # Auto-sizing is off in this suite; we explicitly anchor that since
        # MagicMock(spec=…) returns truthy mocks for unset attributes.
        self.arbitrage_config.auto_size = False
        self.arbitrage_config.max_slippage_pct = Decimal("0")
        self.update_interval = 0.5
        self.executor = ArbitrageExecutor(self.strategy, self.arbitrage_config, self.update_interval)
        self.set_loggers(loggers=[self.executor.logger()])

    @staticmethod
    def create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market

        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="ETH-USDT")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2", "OID-BUY-3"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2", "OID-SELL-3"]
        strategy.cancel.return_value = None
        strategy.connectors = {
            "binance": MagicMock(spec=ConnectorBase),
        }
        return strategy

    def test_is_arbitrage_valid(self):
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'ETH-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-BUSD', 'ETH-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'WETH-USDT'))
        self.assertFalse(self.executor.is_arbitrage_valid('ETH-USDT', 'BTC-USDT'))

    def test_net_pnl_quote(self):
        self.executor.close_type = CloseType.COMPLETED
        self.executor._buy_order = Mock(spec=TrackedOrder)
        self.executor._sell_order = Mock(spec=TrackedOrder)
        self.executor._buy_order.order.executed_amount_base = Decimal('1')
        self.executor._sell_order.order.executed_amount_base = Decimal('1')
        self.executor._buy_order.average_executed_price = Decimal('100')
        self.executor._sell_order.average_executed_price = Decimal('200')
        self.executor._buy_order.cum_fees_quote = Decimal('1')
        self.executor._sell_order.cum_fees_quote = Decimal('1')
        self.executor._status = RunnableStatus.TERMINATED
        self.assertEqual(self.executor.get_net_pnl_quote(), Decimal('98'))
        self.assertEqual(self.executor.get_net_pnl_pct(), Decimal('98'))

    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    @patch.object(ArbitrageExecutor, "get_tx_cost_in_asset")
    async def test_control_task_not_started_not_profitable(self, tx_cost_mock, resulting_price_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.side_effect = [Decimal('100'), Decimal('102')]
        self.executor._status = RunnableStatus.RUNNING
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)

    @patch.object(ArbitrageExecutor, "place_order")
    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    @patch.object(ArbitrageExecutor, "get_tx_cost_in_asset")
    async def test_control_task_profitable(self, tx_cost_mock, resulting_price_mock, place_order_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.side_effect = [Decimal('100'), Decimal('104')]
        place_order_mock.side_effect = ['OID-BUY', 'OID-SELL']
        self.executor._status = RunnableStatus.RUNNING
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(self.executor.buy_order.order_id, 'OID-BUY')
        self.assertEqual(self.executor.sell_order.order_id, 'OID-SELL')

    async def test_control_task_max_retries(self):
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor._cumulative_failures = 4
        await self.executor.control_task()
        self.assertEqual(self.executor.close_type, CloseType.FAILED)
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)

    async def test_control_task_complete(self):
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor._cumulative_failures = 0
        self.executor._buy_order = Mock(spec=TrackedOrder)
        self.executor._sell_order = Mock(spec=TrackedOrder)
        self.executor._buy_order.order.is_filled = True
        self.executor._sell_order.order.is_filled = True
        await self.executor.control_task()
        self.assertEqual(self.executor.close_type, CloseType.COMPLETED)
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)

    def test_to_format_status(self):
        self.executor._status = RunnableStatus.RUNNING
        self.executor._last_buy_price = Decimal('100')
        self.executor._last_sell_price = Decimal('102')
        self.executor._last_tx_cost = Decimal('0.01')
        format_status = "".join(self.executor.to_format_status())
        self.assertIn(f"Arbitrage Status: {RunnableStatus.RUNNING}", format_status)
        self.assertIn("Trade PnL (%): 2.00 % | TX Cost (%): -1.00 % | Net PnL (%): 1.00 %", format_status)

    @patch.object(ArbitrageExecutor, "place_order")
    def test_process_order_failed_event_increments_cumulative_failures(self, _):
        self.executor._cumulative_failures = 0
        self.executor.buy_order.order_id = "123"
        self.executor.sell_order.order_id = "321"
        market = MagicMock()
        buy_order_failed_event = MarketOrderFailureEvent(
            timestamp=123456789,
            order_id=self.executor.buy_order.order_id,
            order_type=OrderType.MARKET,
        )
        self.executor.process_order_failed_event("102", market, buy_order_failed_event)
        self.assertEqual(self.executor._cumulative_failures, 1)

        sell_order_failed_event = MarketOrderFailureEvent(
            timestamp=123456789,
            order_id=self.executor.sell_order.order_id,
            order_type=OrderType.MARKET,
        )
        self.executor.process_order_failed_event("102", market, sell_order_failed_event)
        self.assertEqual(self.executor._cumulative_failures, 2)


class _FakeOrderBook:
    """Minimal stand-in for hummingbot's OrderBook that supports the iterator API."""

    def __init__(self, bids, asks):
        self._bids = list(bids)
        self._asks = list(asks)

    def bid_entries(self):
        for price, amount in self._bids:
            yield _FakeRow(price, amount)

    def ask_entries(self):
        for price, amount in self._asks:
            yield _FakeRow(price, amount)


class _FakeRow:
    def __init__(self, price, amount):
        self.price = price
        self.amount = amount
        self.update_id = 0


class TestArbitrageExecutorAutoSize(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    """Exercises the Option 1 + Option 3 auto-sizing code path."""

    def setUp(self):
        super().setUp()
        self.strategy = self._make_strategy()
        self.config = MagicMock(spec=ArbitrageExecutorConfig)
        # CEX buys, DEX sells → cex_is_buy True.
        self.config.buying_market = ConnectorPair(connector_name="binance", trading_pair="POL-USDT")
        self.config.selling_market = ConnectorPair(connector_name="uniswap_polygon_mainnet",
                                                   trading_pair="WPOL-USDT")
        self.config.min_profitability = Decimal("0.001")  # 10 bps fee proxy
        self.config.order_amount = Decimal("1")
        self.config.gas_conversion_price = Decimal("1")
        self.config.max_retries = 3

        # Auto-sizing knobs.
        self.config.auto_size = True
        self.config.min_order_amount = Decimal("1")
        self.config.max_order_amount = Decimal("100")
        self.config.min_basis_bps = Decimal("0")
        self.config.min_net_profit_quote = Decimal("0")
        self.config.max_balance_pct = Decimal("0.95")
        self.config.max_slippage_pct = Decimal("0.01")
        self.config.max_drift_bps = Decimal("0")  # disable drift check by default
        self.config.tiny_probe_amount = Decimal("0.1")
        self.config.max_walk_levels = 10
        self.config.k_cache_ttl = 30.0

        with patch.object(ArbitrageExecutor, "is_amm_connector",
                          side_effect=lambda exchange: exchange.startswith("uniswap")):
            self.executor = ArbitrageExecutor(self.strategy, self.config, update_interval=1.0)
        self.set_loggers(loggers=[self.executor.logger()])

    @staticmethod
    def _make_strategy():
        strategy = MagicMock(spec=ScriptStrategyBase)
        strategy.buy.side_effect = ["OID-BUY"] * 5
        strategy.sell.side_effect = ["OID-SELL"] * 5
        cex = MagicMock(spec=ConnectorBase)
        cex.get_available_balance.return_value = Decimal("1000")
        dex = MagicMock(spec=ConnectorBase)
        dex.get_available_balance.return_value = Decimal("1000")
        strategy.connectors = {
            "binance": cex,
            "uniswap_polygon_mainnet": dex,
        }
        return strategy

    @patch.object(ArbitrageExecutor, "is_amm_connector",
                  side_effect=lambda exchange: exchange.startswith("uniswap"))
    def test_size_band_uses_min_max_when_auto_size(self, _):
        self.assertEqual(self.executor._size_band(), (Decimal("1"), Decimal("100")))

    def test_auto_size_disabled_falls_back_to_fixed_path(self):
        self.config.auto_size = False
        self.assertFalse(self.executor._auto_size_enabled)

    @patch.object(ArbitrageExecutor, "is_amm_connector",
                  side_effect=lambda exchange: exchange.startswith("uniswap"))
    def test_cex_dex_split_buy_cex_sell_dex(self, _):
        cex_c, dex_c, cex_p, dex_p, cex_is_buy = self.executor._cex_dex_split()
        self.assertEqual(cex_c, "binance")
        self.assertEqual(dex_c, "uniswap_polygon_mainnet")
        self.assertEqual(cex_p, "POL-USDT")
        self.assertEqual(dex_p, "WPOL-USDT")
        self.assertTrue(cex_is_buy)

    def test_cex_zone_levels_filters_asks_above_threshold(self):
        ob = _FakeOrderBook(bids=[], asks=[
            (Decimal("100"), Decimal("5")),
            (Decimal("101"), Decimal("10")),
            (Decimal("103"), Decimal("20")),  # past the threshold
            (Decimal("105"), Decimal("30")),
        ])
        zone = ArbitrageExecutor._cex_zone_levels(
            ob, cex_is_buy=True, buy_probe=Decimal("100"), sell_probe=Decimal("103"))
        self.assertEqual(len(zone), 2)
        self.assertEqual(zone[0], (Decimal("100"), Decimal("5")))
        self.assertEqual(zone[1], (Decimal("101"), Decimal("10")))

    def test_cex_zone_levels_filters_bids_below_threshold(self):
        ob = _FakeOrderBook(bids=[
            (Decimal("105"), Decimal("3")),
            (Decimal("104"), Decimal("7")),
            (Decimal("100"), Decimal("9")),  # at threshold, excluded
        ], asks=[])
        zone = ArbitrageExecutor._cex_zone_levels(
            ob, cex_is_buy=False, buy_probe=Decimal("100"), sell_probe=Decimal("110"))
        self.assertEqual(len(zone), 2)
        self.assertEqual(zone[0], (Decimal("105"), Decimal("3")))

    def test_expected_price_with_slippage_on_dex_buy(self):
        adj = self.executor._expected_price_with_slippage(Decimal("100"), is_buy=True, is_amm=True)
        self.assertEqual(adj, Decimal("101"))

    def test_expected_price_with_slippage_on_dex_sell(self):
        adj = self.executor._expected_price_with_slippage(Decimal("100"), is_buy=False, is_amm=True)
        self.assertEqual(adj, Decimal("99"))

    def test_expected_price_with_slippage_pass_through_for_cex(self):
        adj = self.executor._expected_price_with_slippage(Decimal("100"), is_buy=True, is_amm=False)
        self.assertEqual(adj, Decimal("100"))

    def test_analytic_gate_below_basis_floor_returns_false(self):
        self.config.min_basis_bps = Decimal("50")  # 50 bps floor
        self.assertFalse(self.executor._analytic_gate(Decimal("0.001")))  # 10 bps

    def test_analytic_gate_passes_when_no_k_cached(self):
        self.executor._k_cached = None
        self.assertTrue(self.executor._analytic_gate(Decimal("0.01")))

    def test_refresh_k_cache_estimates_positive_k_from_decreasing_basis(self):
        samples = [(Decimal("1"), Decimal("0.02")), (Decimal("10"), Decimal("0.012"))]
        self.executor._refresh_k_cache(samples)
        # slope = (0.012 - 0.02) / (10 - 1) = -0.000889 → k ≈ 0.000889
        self.assertIsNotNone(self.executor._k_cached)
        self.assertGreater(self.executor._k_cached, 0)

    def test_refresh_k_cache_invalidates_on_increasing_basis(self):
        # Pre-seed with a stale positive k.
        self.executor._k_cached = Decimal("0.001")
        samples = [(Decimal("1"), Decimal("0.01")), (Decimal("10"), Decimal("0.02"))]
        self.executor._refresh_k_cache(samples)
        # Increasing basis with size means model invalidated → cache cleared.
        self.assertIsNone(self.executor._k_cached)

    def test_refresh_k_cache_invalidates_on_identical_sizes(self):
        self.executor._k_cached = Decimal("0.001")
        # All samples at the same cumulative size (zero variance).
        samples = [(Decimal("5"), Decimal("0.02")), (Decimal("5"), Decimal("0.018"))]
        self.executor._refresh_k_cache(samples)
        self.assertIsNone(self.executor._k_cached)

    def test_refresh_k_cache_uses_all_samples_via_ols(self):
        # Three points exactly on the line basis = 0.02 - 0.001 * s.
        samples = [
            (Decimal("1"), Decimal("0.019")),
            (Decimal("5"), Decimal("0.015")),
            (Decimal("10"), Decimal("0.010")),
        ]
        self.executor._refresh_k_cache(samples)
        self.assertIsNotNone(self.executor._k_cached)
        self.assertAlmostEqual(float(self.executor._k_cached), 0.001, places=4)

    def test_apply_constraints_rejects_size_below_min(self):
        ok = self.executor._apply_constraints(Decimal("0.5"), Decimal("100"), Decimal("50"))
        self.assertFalse(ok)
        self.assertIn("< min_order_amount", self.executor._auto_size_skip_reason)

    def test_apply_constraints_rejects_negative_net_profit(self):
        self.config.min_net_profit_quote = Decimal("1")
        ok = self.executor._apply_constraints(Decimal("5"), Decimal("0.5"), Decimal("50"))
        self.assertFalse(ok)
        self.assertIn("min_net_profit_quote", self.executor._auto_size_skip_reason)

    def test_apply_constraints_rejects_when_sell_side_base_insufficient(self):
        # Make the sell-side base balance tiny.
        self.strategy.connectors["uniswap_polygon_mainnet"].get_available_balance.return_value = Decimal("1")
        ok = self.executor._apply_constraints(Decimal("50"), Decimal("10"), Decimal("100"))
        self.assertFalse(ok)
        self.assertIn("sell-side base balance", self.executor._auto_size_skip_reason)

    def test_apply_constraints_rejects_when_buy_side_quote_insufficient(self):
        # Sell-side base ample, buy-side quote tiny.
        self.strategy.connectors["uniswap_polygon_mainnet"].get_available_balance.return_value = Decimal("10000")
        self.strategy.connectors["binance"].get_available_balance.return_value = Decimal("5")
        # size 10 @ buy_vwap 100 → 1000 quote needed, only 5 available.
        ok = self.executor._apply_constraints(Decimal("10"), Decimal("10"), Decimal("100"))
        self.assertFalse(ok)
        self.assertIn("buy-side quote balance", self.executor._auto_size_skip_reason)

    def test_apply_constraints_uses_fresh_buy_vwap_not_stale_last_buy_price(self):
        """H-1 regression: balance check must use the per-tick buy_vwap, not
        the stale _last_buy_price (which defaults to 1 on the very first tick
        and would silently bypass the buy-side reserve for any token > 1)."""
        # _last_buy_price still at constructor default = Decimal('1').
        self.assertEqual(self.executor._last_buy_price, Decimal("1"))
        # Buy-side quote balance is 50; if the OLD code path (using
        # _last_buy_price = 1) ran, required_quote = 50 × 1 = 50 ≤ reserve × 50
        # and the check would pass. With the fix, required_quote = 50 × 1000
        # → 50,000 → fails.
        self.strategy.connectors["uniswap_polygon_mainnet"].get_available_balance.return_value = Decimal("10000")
        self.strategy.connectors["binance"].get_available_balance.return_value = Decimal("50")
        ok = self.executor._apply_constraints(Decimal("50"), Decimal("10"), Decimal("1000"))
        self.assertFalse(ok)
        self.assertIn("buy-side quote balance", self.executor._auto_size_skip_reason)

    def test_apply_constraints_rejects_invalid_buy_vwap(self):
        ok = self.executor._apply_constraints(Decimal("5"), Decimal("10"), Decimal("0"))
        self.assertFalse(ok)
        self.assertIn("invalid buy_vwap", self.executor._auto_size_skip_reason)

    async def test_drift_check_passes_when_disabled(self):
        self.config.max_drift_bps = Decimal("0")
        self.executor._last_dex_spot_at_walk = Decimal("100")
        self.assertTrue(await self.executor._drift_check())

    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_drift_check_fails_when_spot_moves_too_much(self, price_mock):
        price_mock.return_value = Decimal("105")
        self.config.max_drift_bps = Decimal("100")  # 1% max
        self.executor._last_dex_spot_at_walk = Decimal("100")
        ok = await self.executor._drift_check()
        self.assertFalse(ok)
        self.assertIn("drift", self.executor._auto_size_skip_reason)

    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_drift_check_passes_within_band(self, price_mock):
        price_mock.return_value = Decimal("100.1")
        self.config.max_drift_bps = Decimal("100")
        self.executor._last_dex_spot_at_walk = Decimal("100")
        self.assertTrue(await self.executor._drift_check())

    @patch.object(ArbitrageExecutor, "_safe_get_order_book")
    @patch.object(ArbitrageExecutor, "_update_tx_cost_for_amount")
    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_compute_optimal_size_picks_size_at_peak(self, price_mock, tx_cost_mock, ob_mock):
        # As we eat the cheap CEX asks and quote larger sizes on DEX, the DEX
        # VWAP creeps DOWN (we're selling more into worse bids).
        ob_mock.return_value = _FakeOrderBook(
            bids=[],
            asks=[
                (Decimal("100"), Decimal("5")),
                (Decimal("100.5"), Decimal("5")),
                (Decimal("101"), Decimal("5")),
                (Decimal("101.5"), Decimal("5")),
                (Decimal("110"), Decimal("100")),  # past zone threshold
            ],
        )
        # Sequence of DEX sell quotes for cum_size = 5, 10, 15, 20 base.
        price_mock.side_effect = [
            Decimal("103"),    # cum 5  → +3 USDT per unit
            Decimal("102.8"),  # cum 10 → still profitable
            Decimal("102.0"),  # cum 15 → margin shrinking
            Decimal("101.0"),  # cum 20 → past peak
        ]
        self.executor._last_tx_cost = Decimal("0.1")

        async def _noop(*_args, **_kwargs):
            return None
        tx_cost_mock.side_effect = _noop

        result = await self.executor._compute_optimal_size(
            initial_buy_price=Decimal("100"),
            initial_sell_price=Decimal("103"),
            conversion_rate=Decimal("1"),
        )
        self.assertIsNotNone(result)
        size, net, buy_vwap, sell_vwap, samples = result
        self.assertGreater(size, Decimal("0"))
        self.assertGreater(net, 0)
        self.assertGreater(sell_vwap, buy_vwap)
        self.assertGreaterEqual(len(samples), 2)

    @patch.object(ArbitrageExecutor, "_safe_get_order_book")
    @patch.object(ArbitrageExecutor, "_update_tx_cost_for_amount")
    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_compute_optimal_size_returns_none_when_zone_empty(self, price_mock, tx_cost_mock, ob_mock):
        # Best ask above the sell-side probe → empty profitable zone.
        ob_mock.return_value = _FakeOrderBook(bids=[], asks=[(Decimal("200"), Decimal("5"))])

        async def _noop(*_a, **_kw):
            return None
        tx_cost_mock.side_effect = _noop

        result = await self.executor._compute_optimal_size(
            initial_buy_price=Decimal("100"),
            initial_sell_price=Decimal("101"),
            conversion_rate=Decimal("1"),
        )
        self.assertIsNone(result)

    @patch.object(ArbitrageExecutor, "_safe_get_order_book")
    @patch.object(ArbitrageExecutor, "_update_tx_cost_for_amount")
    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_compute_optimal_size_skips_zero_amount_levels(self, price_mock, tx_cost_mock, ob_mock):
        ob_mock.return_value = _FakeOrderBook(
            bids=[],
            asks=[
                (Decimal("100"), Decimal("0")),    # zero amount — must be skipped
                (Decimal("100.1"), Decimal("5")),
                (Decimal("100.2"), Decimal("5")),
            ],
        )
        # Only 2 DEX quote calls expected — one per non-zero level.
        price_mock.side_effect = [Decimal("103"), Decimal("102.5")]
        self.executor._last_tx_cost = Decimal("0.1")

        async def _noop(*_a, **_kw):
            return None
        tx_cost_mock.side_effect = _noop

        result = await self.executor._compute_optimal_size(
            initial_buy_price=Decimal("100"),
            initial_sell_price=Decimal("103"),
            conversion_rate=Decimal("1"),
        )
        self.assertIsNotNone(result)
        size, _, _, _, _ = result
        # The walker should have consumed both non-zero levels (cum 5 then 10)
        # and selected the more profitable one. Exactly 2 DEX quote calls
        # confirms the zero-amount level was skipped without an RPC.
        self.assertEqual(price_mock.call_count, 2)
        self.assertIn(size, {Decimal("5"), Decimal("10")})

    @patch.object(ArbitrageExecutor, "_safe_get_order_book")
    @patch.object(ArbitrageExecutor, "_update_tx_cost_for_amount")
    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_compute_optimal_size_breaks_when_dex_quote_returns_none(
            self, price_mock, tx_cost_mock, ob_mock):
        ob_mock.return_value = _FakeOrderBook(
            bids=[],
            asks=[
                (Decimal("100"), Decimal("5")),
                (Decimal("100.5"), Decimal("5")),
            ],
        )
        price_mock.side_effect = [Decimal("103"), None]
        self.executor._last_tx_cost = Decimal("0.1")

        async def _noop(*_a, **_kw):
            return None
        tx_cost_mock.side_effect = _noop

        result = await self.executor._compute_optimal_size(
            initial_buy_price=Decimal("100"),
            initial_sell_price=Decimal("103"),
            conversion_rate=Decimal("1"),
        )
        # First level cleared min_size (1) → best was recorded → result not None.
        self.assertIsNotNone(result)
        size, _, _, _, _ = result
        self.assertEqual(size, Decimal("5"))

    @patch.object(ArbitrageExecutor, "_safe_get_order_book")
    @patch.object(ArbitrageExecutor, "_update_tx_cost_for_amount")
    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_compute_optimal_size_does_not_double_subtract_cex_fee(
            self, price_mock, tx_cost_mock, ob_mock):
        """H-2 regression: net_quote = gross_quote − tx_cost only. The
        previous version also subtracted min_profitability × notional, which
        double-counted the CEX fee that update_tx_cost already includes."""
        ob_mock.return_value = _FakeOrderBook(
            bids=[], asks=[(Decimal("100"), Decimal("10"))],
        )
        price_mock.side_effect = [Decimal("110")]
        # min_profitability is 10 bps in this suite. If the buggy formula
        # were still in place, net would be:
        #     gross  = (110 − 100) × 10 = 100
        #     bug    = 100 − tx_cost − 0.001 × 100 × 10 = 100 − tx_cost − 1
        # With the fix:
        #     net    = 100 − tx_cost
        self.executor._last_tx_cost = Decimal("0.5")

        async def _noop(*_a, **_kw):
            return None
        tx_cost_mock.side_effect = _noop

        result = await self.executor._compute_optimal_size(
            initial_buy_price=Decimal("100"),
            initial_sell_price=Decimal("110"),
            conversion_rate=Decimal("1"),
        )
        self.assertIsNotNone(result)
        size, net, _, _, _ = result
        self.assertEqual(size, Decimal("10"))
        # gross 100 − tx_cost 0.5 = 99.5; buggy version would yield 98.5.
        self.assertEqual(net, Decimal("99.5"))

    @patch.object(ArbitrageExecutor, "_safe_get_order_book")
    @patch.object(ArbitrageExecutor, "_update_tx_cost_for_amount")
    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_compute_optimal_size_clamps_to_max_size(
            self, price_mock, tx_cost_mock, ob_mock):
        self.config.max_order_amount = Decimal("7")  # clamp below the level depth
        ob_mock.return_value = _FakeOrderBook(
            bids=[], asks=[(Decimal("100"), Decimal("100"))],
        )
        price_mock.side_effect = [Decimal("110")]
        self.executor._last_tx_cost = Decimal("0.1")

        async def _noop(*_a, **_kw):
            return None
        tx_cost_mock.side_effect = _noop

        result = await self.executor._compute_optimal_size(
            initial_buy_price=Decimal("100"),
            initial_sell_price=Decimal("110"),
            conversion_rate=Decimal("1"),
        )
        self.assertIsNotNone(result)
        size, _, _, _, _ = result
        self.assertEqual(size, Decimal("7"))

    @patch.object(ArbitrageExecutor, "is_amm_connector",
                  side_effect=lambda exchange: exchange.startswith("uniswap"))
    @patch.object(ArbitrageExecutor, "place_order")
    @patch.object(ArbitrageExecutor, "_drift_check")
    @patch.object(ArbitrageExecutor, "_compute_optimal_size")
    @patch.object(ArbitrageExecutor, "_get_prices_for_amount")
    @patch.object(ArbitrageExecutor, "get_quote_asset_conversion_rate")
    async def test_auto_size_control_task_executes_when_all_checks_pass(
            self, conv_mock, prices_mock, walk_mock, drift_mock, place_mock, _amm_mock):
        # Provide enough balance on both sides for the auto-size constraint
        # check to pass at the chosen (size=20, buy_vwap=100) → 2000 quote.
        self.strategy.connectors["binance"].get_available_balance.return_value = Decimal("10000")
        self.strategy.connectors["uniswap_polygon_mainnet"].get_available_balance.return_value = Decimal("10000")

        async def _conv():
            return Decimal("1")
        conv_mock.side_effect = _conv

        async def _prices(_amt):
            return Decimal("100"), Decimal("105")
        prices_mock.side_effect = _prices

        async def _walk(*_a, **_kw):
            return (Decimal("20"), Decimal("12"), Decimal("100"), Decimal("105"),
                    [(Decimal("1"), Decimal("0.05")), (Decimal("20"), Decimal("0.03"))])
        walk_mock.side_effect = _walk

        async def _drift(*_a, **_kw):
            return True
        drift_mock.side_effect = _drift

        place_mock.side_effect = ["OID-BUY", "OID-SELL"]
        self.executor._status = RunnableStatus.RUNNING
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(self.executor._optimal_size, Decimal("20"))
        self.assertEqual(self.executor.buy_order.order_id, "OID-BUY")
        self.assertEqual(self.executor.sell_order.order_id, "OID-SELL")
        self.assertIsNotNone(self.executor._k_cached)
        # Drift check must have been called with the chosen size and the
        # DEX-side VWAP (selling on DEX → expected = sell_vwap).
        called_kwargs = drift_mock.call_args.kwargs
        self.assertEqual(called_kwargs.get("size"), Decimal("20"))
        self.assertEqual(called_kwargs.get("expected_dex_vwap"), Decimal("105"))

    @patch.object(ArbitrageExecutor, "place_order")
    @patch.object(ArbitrageExecutor, "_drift_check")
    @patch.object(ArbitrageExecutor, "_compute_optimal_size")
    @patch.object(ArbitrageExecutor, "_get_prices_for_amount")
    @patch.object(ArbitrageExecutor, "get_quote_asset_conversion_rate")
    async def test_auto_size_control_task_skips_when_drift_fails(
            self, conv_mock, prices_mock, walk_mock, drift_mock, place_mock):
        async def _conv():
            return Decimal("1")
        conv_mock.side_effect = _conv

        async def _prices(_amt):
            return Decimal("100"), Decimal("105")
        prices_mock.side_effect = _prices

        async def _walk(*_a, **_kw):
            return (Decimal("20"), Decimal("12"), Decimal("100"), Decimal("105"),
                    [(Decimal("1"), Decimal("0.05"))])
        walk_mock.side_effect = _walk

        async def _drift(*_a, **_kw):
            return False
        drift_mock.side_effect = _drift

        self.executor._status = RunnableStatus.RUNNING
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)
        place_mock.assert_not_called()

    @patch.object(ArbitrageExecutor, "get_quote_asset_conversion_rate")
    @patch.object(ArbitrageExecutor, "_get_prices_for_amount")
    async def test_auto_size_control_task_skips_when_conversion_rate_fails(
            self, prices_mock, conv_mock):
        async def _prices(_amt):
            return Decimal("100"), Decimal("105")
        prices_mock.side_effect = _prices

        async def _conv_raises():
            raise RuntimeError("rate oracle down")
        conv_mock.side_effect = _conv_raises

        self.executor._status = RunnableStatus.RUNNING
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)
        self.assertIsNotNone(self.executor._auto_size_skip_reason)
        self.assertIn("conversion rate failed", self.executor._auto_size_skip_reason)

    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_drift_check_size_stage_fails_when_dex_vwap_moved(self, price_mock):
        # Stage 1 (spot) returns within band; Stage 2 (size) returns outside band.
        self.config.max_drift_bps = Decimal("100")  # 1%
        self.config.max_slippage_pct = Decimal("0.005")  # 50 bps
        self.executor._last_dex_spot_at_walk = Decimal("100")
        price_mock.side_effect = [
            Decimal("100.05"),  # stage 1 spot
            Decimal("99.0"),    # stage 2 size — 100 bps below expected
        ]
        ok = await self.executor._drift_check(
            size=Decimal("10"), expected_dex_vwap=Decimal("100"))
        self.assertFalse(ok)
        self.assertIn("dex_vwap@size drift", self.executor._auto_size_skip_reason)

    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    async def test_drift_check_size_stage_passes_within_slippage(self, price_mock):
        self.config.max_drift_bps = Decimal("100")
        self.config.max_slippage_pct = Decimal("0.01")  # 100 bps
        self.executor._last_dex_spot_at_walk = Decimal("100")
        price_mock.side_effect = [
            Decimal("100.05"),  # stage 1 spot
            Decimal("99.5"),    # stage 2 size — 50 bps below expected, within band
        ]
        ok = await self.executor._drift_check(
            size=Decimal("10"), expected_dex_vwap=Decimal("100"))
        self.assertTrue(ok)

    def test_analytic_gate_with_cached_k_rejects_below_peak_profit(self):
        # Cached k=0.001/unit, basis 50 bps, fees 10 bps → adjusted basis 40 bps.
        # Peak size  s* = adjusted / (2k) = 0.004 / 0.002 = 2.
        # Peak net  = s* (adjusted - k s*) - tx_cost
        #          = 2 (0.004 - 0.001*2) - last_tx_cost
        #          = 2 * 0.002 - last_tx_cost = 0.004 - last_tx_cost
        # With min_net_profit_quote = 1, peak = -0.996 → gate rejects.
        self.executor._k_cached = Decimal("0.001")
        self.executor._k_cached_ts = 9.99e9  # far future
        self.executor._last_tx_cost = Decimal("0")
        self.config.min_net_profit_quote = Decimal("1")
        self.assertFalse(self.executor._analytic_gate(Decimal("0.005")))

    def test_analytic_gate_with_cached_k_passes_when_peak_clears_floor(self):
        # basis 5%, fees 10 bps → adjusted 4.9%; k=0.0001 → peak size 245.
        # Peak net ≈ 245 * (0.049 - 0.0001*245)/2 - tx_cost
        # which is well above 0. Gate passes.
        self.executor._k_cached = Decimal("0.0001")
        self.executor._k_cached_ts = 9.99e9
        self.executor._last_tx_cost = Decimal("0")
        self.config.min_net_profit_quote = Decimal("1")
        self.assertTrue(self.executor._analytic_gate(Decimal("0.05")))

    def test_get_custom_info_includes_auto_size_fields(self):
        self.executor._optimal_size = Decimal("12")
        self.executor._last_net_profit_quote = Decimal("3.4")
        self.executor._k_cached = Decimal("0.002")
        self.executor._auto_size_skip_reason = "test reason"
        self.executor._last_tx_cost = Decimal("0.12")
        info = self.executor.get_custom_info()
        self.assertTrue(info["auto_size"])
        self.assertEqual(info["optimal_size"], Decimal("12"))
        self.assertEqual(info["last_net_profit_quote"], Decimal("3.4"))
        self.assertEqual(info["k_cached"], Decimal("0.002"))
        self.assertEqual(info["skip_reason"], "test reason")
        # tx_cost_pct must use _arb_amount() = _optimal_size, not order_amount.
        self.assertEqual(info["tx_cost_pct"], Decimal("0.12") / Decimal("12"))


class TestArbitrageExecutorConfigValidator(IsolatedAsyncioWrapperTestCase):
    """Round-trip the real pydantic config to exercise `_validate_auto_size_band`."""

    def _base_kwargs(self):
        return dict(
            buying_market=ConnectorPair(connector_name="binance", trading_pair="POL-USDT"),
            selling_market=ConnectorPair(connector_name="uniswap_polygon_mainnet",
                                         trading_pair="WPOL-USDT"),
            order_amount=Decimal("10"),
            min_profitability=Decimal("0.001"),
        )

    def test_validator_accepts_auto_size_off(self):
        cfg = ArbitrageExecutorConfig(**self._base_kwargs())
        self.assertFalse(cfg.auto_size)

    def test_validator_accepts_valid_band(self):
        cfg = ArbitrageExecutorConfig(
            **self._base_kwargs(),
            auto_size=True,
            min_order_amount=Decimal("1"),
            max_order_amount=Decimal("50"),
        )
        self.assertEqual(cfg.min_order_amount, Decimal("1"))
        self.assertEqual(cfg.max_order_amount, Decimal("50"))

    def test_validator_rejects_zero_min(self):
        with self.assertRaises(Exception) as cm:
            ArbitrageExecutorConfig(
                **self._base_kwargs(),
                auto_size=True,
                min_order_amount=Decimal("0"),
                max_order_amount=Decimal("10"),
            )
        self.assertIn("min_order_amount", str(cm.exception))

    def test_validator_rejects_max_below_min(self):
        with self.assertRaises(Exception) as cm:
            ArbitrageExecutorConfig(
                **self._base_kwargs(),
                auto_size=True,
                min_order_amount=Decimal("10"),
                max_order_amount=Decimal("5"),
            )
        self.assertIn("max_order_amount", str(cm.exception))

    def test_validator_falls_back_to_order_amount_when_band_unset(self):
        cfg = ArbitrageExecutorConfig(
            **self._base_kwargs(),
            auto_size=True,
        )
        # No explicit min/max → defaults to None, validator uses order_amount.
        self.assertIsNone(cfg.min_order_amount)
        self.assertIsNone(cfg.max_order_amount)
        self.assertTrue(cfg.auto_size)

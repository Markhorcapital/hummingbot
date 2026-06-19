import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import BuyOrderCompletedEvent
from hummingbot.strategy_v2.executors.convergence_executor.convergence_executor import ConvergenceExecutor
from hummingbot.strategy_v2.executors.convergence_executor.data_types import ConvergenceExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


class _FakeRow:
    def __init__(self, price, amount):
        self.price = price
        self.amount = amount


class _FakeOrderBook:
    def __init__(self, bids=None, asks=None):
        self._bids = bids or []
        self._asks = asks or []

    def bid_entries(self):
        return self._bids

    def ask_entries(self):
        return self._asks


class TestConvergenceExecutorHelpers(unittest.TestCase):
    def _config(self):
        return ConvergenceExecutorConfig(
            connector_name="gate_io",
            trading_pair="ALI-USDT",
            dex_rpc_url="http://localhost:8545",
            dex_pool_address="0xpool",
            dex_base_token_address="0xbase",
            dex_quote_token_address="0xquote",
            min_gap_bps=Decimal("30"),
            min_edge_bps_after_fees=Decimal("0"),
            max_amount_quote=Decimal("100"),
        )

    def test_zone_levels_sell_includes_bids_at_or_above_dex_fair(self):
        book = _FakeOrderBook(
            bids=[
                _FakeRow("0.00160", "10"),
                _FakeRow("0.00155", "5"),
                _FakeRow("0.00148", "100"),
            ],
        )
        levels = ConvergenceExecutor._zone_levels(book, TradeType.SELL, Decimal("0.00150"))
        prices = [p for p, _ in levels]
        self.assertEqual(prices, [Decimal("0.00160"), Decimal("0.00155")])

    def test_zone_levels_buy_includes_asks_at_or_below_dex_fair(self):
        book = _FakeOrderBook(
            asks=[
                _FakeRow("0.00140", "10"),
                _FakeRow("0.00145", "5"),
                _FakeRow("0.00155", "100"),
            ],
        )
        levels = ConvergenceExecutor._zone_levels(book, TradeType.BUY, Decimal("0.00150"))
        prices = [p for p, _ in levels]
        self.assertEqual(prices, [Decimal("0.00140"), Decimal("0.00145")])

    @patch.object(ConvergenceExecutor, "get_order_book")
    def test_compute_zone_quote_sums_zone_levels(self, book_mock):
        book_mock.return_value = _FakeOrderBook(
            bids=[
                _FakeRow("0.00160", "10"),
                _FakeRow("0.00155", "5"),
                _FakeRow("0.00140", "100"),
            ],
        )
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.max_walk_levels = 8
        zone_quote = executor._compute_zone_quote(Decimal("0.00150"), TradeType.SELL)
        self.assertEqual(zone_quote, Decimal("0.02375"))

    def test_pick_side_sell_when_cex_above_dex(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor._last_gap_bps = None
        executor._skip_reason = None
        side = executor._pick_side(Decimal("0.00155"), Decimal("0.00150"))
        self.assertEqual(side, TradeType.SELL)

    def test_pick_side_buy_when_cex_below_dex(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor._last_gap_bps = None
        executor._skip_reason = None
        side = executor._pick_side(Decimal("0.00145"), Decimal("0.00150"))
        self.assertEqual(side, TradeType.BUY)

    def test_gap_widening_allows_when_above_spawn_ref(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.require_gap_widening = True
        executor.config.spawn_gap_bps = Decimal("30")
        executor._widening_ref_bps = Decimal("30")
        self.assertTrue(
            executor._gap_widening_allows_trade(Decimal("0.0014945"), Decimal("0.00150"))
        )

    def test_gap_widening_blocks_when_not_above_spawn_ref(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.require_gap_widening = True
        executor.config.spawn_gap_bps = Decimal("40")
        executor._widening_ref_bps = Decimal("40")
        # ~33 bps abs gap — below 40 spawn ref
        self.assertFalse(
            executor._gap_widening_allows_trade(Decimal("0.001501"), Decimal("0.00150"))
        )

    def test_gap_widening_disabled_always_allows(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.require_gap_widening = False
        executor._widening_ref_bps = Decimal("100")
        self.assertTrue(
            executor._gap_widening_allows_trade(Decimal("0.001501"), Decimal("0.00150"))
        )

    def test_pick_side_none_when_gap_too_small(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor._last_gap_bps = None
        executor._skip_reason = None
        side = executor._pick_side(Decimal("0.001501"), Decimal("0.00150"))
        self.assertIsNone(side)
        self.assertIn("min_gap_bps", executor._skip_reason)

    @patch.object(ConvergenceExecutor, "get_order_book")
    def test_rebalance_chunk_size_uses_zone_up_to_quote_cap(self, book_mock):
        book_mock.return_value = _FakeOrderBook(
            bids=[_FakeRow("0.00155", "20")],
        )
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.rebalance_only = True
        executor.config.max_amount_quote = Decimal("100")
        executor.config.sweep_chunks = 1
        executor._chunks_remaining = 1
        connector = MagicMock()
        connector.get_available_balance.return_value = Decimal("1000")
        connector.quantize_order_amount.side_effect = lambda _p, a: a
        executor.connectors = {"gate_io": connector}
        trading_rules = MagicMock()
        trading_rules.min_order_size = Decimal("1")
        executor.get_trading_rules = MagicMock(return_value=trading_rules)

        size, vwap, zone_quote_est = executor._compute_chunk_size(Decimal("0.00150"), TradeType.SELL)
        self.assertEqual(size, Decimal("20"))
        self.assertEqual(vwap, Decimal("0.00155"))
        self.assertEqual(zone_quote_est, Decimal("0.031"))

    @patch.object(ConvergenceExecutor, "get_order_book")
    @patch.object(ConvergenceExecutor, "_estimate_taker_fee_quote")
    def test_rebalance_chunk_size_ignores_fee_when_rebalance_only(self, fee_mock, book_mock):
        fee_mock.return_value = Decimal("10")
        book_mock.return_value = _FakeOrderBook(
            bids=[_FakeRow("0.00155", "20")],
        )
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.rebalance_only = True
        executor.config.sweep_chunks = 1
        executor._chunks_remaining = 1
        connector = MagicMock()
        connector.get_available_balance.return_value = Decimal("1000")
        connector.quantize_order_amount.side_effect = lambda _p, a: a
        executor.connectors = {"gate_io": connector}
        trading_rules = MagicMock()
        trading_rules.min_order_size = Decimal("1")
        executor.get_trading_rules = MagicMock(return_value=trading_rules)

        size, _, _ = executor._compute_chunk_size(Decimal("0.00150"), TradeType.SELL)
        self.assertGreater(size, 0)
        fee_mock.assert_not_called()

    @patch.object(ConvergenceExecutor, "get_order_book")
    @patch.object(ConvergenceExecutor, "_estimate_taker_fee_quote")
    def test_profit_chunk_size_zero_when_net_edge_below_floor(self, fee_mock, book_mock):
        fee_mock.return_value = Decimal("1.0")
        book_mock.return_value = _FakeOrderBook(
            bids=[_FakeRow("0.00155", "20")],
        )
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.rebalance_only = False
        executor.config.min_edge_bps_after_fees = Decimal("10")
        executor.config.sweep_chunks = 1
        executor._chunks_remaining = 1
        connector = MagicMock()
        connector.get_available_balance.return_value = Decimal("1000")
        connector.quantize_order_amount.side_effect = lambda _p, a: a
        executor.connectors = {"gate_io": connector}
        trading_rules = MagicMock()
        trading_rules.min_order_size = Decimal("1")
        executor.get_trading_rules = MagicMock(return_value=trading_rules)

        size, _, net = executor._compute_chunk_size(Decimal("0.00150"), TradeType.SELL)
        self.assertEqual(size, Decimal("0"))
        self.assertEqual(net, Decimal("0"))

    def test_apply_exit_close_type_position_hold_when_filled(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor._total_filled_base = Decimal("10")
        executor.close_type = None
        executor._apply_exit_close_type()
        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)

    def test_apply_exit_close_type_completed_when_no_fill(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor._total_filled_base = Decimal("0")
        executor.close_type = None
        executor._apply_exit_close_type()
        self.assertEqual(executor.close_type, CloseType.COMPLETED)

    def test_finish_sweep_position_hold_after_partial_fill(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor._total_filled_base = Decimal("5")
        executor._sweep_complete = False
        executor._status = RunnableStatus.RUNNING
        executor._finish_sweep()
        self.assertTrue(executor._sweep_complete)
        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)

    def test_early_stop_position_hold_when_fills_exist(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor._order = None
        executor._total_filled_base = Decimal("1")
        executor._sweep_complete = False
        executor._status = RunnableStatus.RUNNING
        executor.early_stop()
        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)

    def test_get_custom_info_exposes_held_orders_and_side(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.side = TradeType.SELL
        executor._sweep_side = TradeType.SELL
        executor._held_position_orders = [{"client_order_id": "oid-1"}]
        executor._last_dex_fair = None
        executor._last_cex_mid = None
        executor._last_gap_bps = None
        executor._chunks_remaining = 0
        executor._total_filled_base = Decimal("0")
        executor._total_filled_quote = Decimal("0")
        executor._cum_fees_quote = Decimal("0")
        executor._skip_reason = None
        executor._widening_ref_bps = None
        info = executor.get_custom_info()
        self.assertEqual(info["held_position_orders"], [{"client_order_id": "oid-1"}])
        self.assertEqual(info["side"], TradeType.SELL)

    def test_cum_fees_includes_completed_chunks(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor._cum_fees_quote = Decimal("0.5")
        executor._order = None
        self.assertEqual(executor.get_cum_fees_quote(), Decimal("0.5"))

    def test_get_net_pnl_pct_tolerates_nan_filled_quote(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor._last_dex_fair = Decimal("0.0014")
        executor._total_filled_base = Decimal("1000")
        executor._total_filled_quote = Decimal("NaN")
        executor._sweep_side = TradeType.BUY
        executor._cum_fees_quote = Decimal("0")
        executor._order = None
        self.assertEqual(executor.get_net_pnl_pct(), Decimal("0"))
        self.assertEqual(executor.get_net_pnl_quote(), Decimal("0"))
        self.assertEqual(executor.filled_amount_quote, Decimal("0"))

    def test_chunk_fill_quote_uses_completed_event_quote_not_nan_price(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        tracked = MagicMock()
        tracked.order = MagicMock()
        tracked.order.executed_amount_base = Decimal("1000")
        tracked.order.executed_amount_quote = Decimal("0")
        tracked.average_executed_price = Decimal("NaN")
        event = BuyOrderCompletedEvent(
            timestamp=1.0,
            order_id="oid",
            base_asset="ALI",
            quote_asset="USDT",
            base_asset_amount=Decimal("1000"),
            quote_asset_amount=Decimal("1.33"),
            order_type=OrderType.MARKET,
        )
        self.assertEqual(executor._chunk_fill_quote(event, tracked), Decimal("1.33"))

    def test_running_step_waits_when_gap_not_widening(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.require_gap_widening = True
        executor.config.spawn_gap_bps = Decimal("50")
        executor.config.zone_quote_threshold_usdt = Decimal("200")
        executor.config.sweep_chunks = 1
        executor._sweep_complete = False
        executor._order = None
        executor._sweep_side = TradeType.BUY
        executor._chunks_remaining = 1
        executor._widening_ref_bps = Decimal("50")
        executor._last_wait_log_key = None
        executor._last_wait_log_ts = 0.0
        executor._skip_reason = None
        executor._current_retries = 0
        executor._max_retries = 10
        executor._status = RunnableStatus.RUNNING
        executor._total_filled_base = Decimal("0")
        executor.close_type = None

        dex_feed = MagicMock()
        dex_feed.should_poll.return_value = False
        dex_feed.is_stale.return_value = False
        dex_feed.get_dex_fair.return_value = Decimal("0.00150")
        dex_feed.sanity_ok.return_value = True
        executor._dex_feed = dex_feed

        executor._ensure_dex_feed = MagicMock(return_value=True)
        executor._cex_mid = MagicMock(return_value=Decimal("0.001501"))
        executor._poll_dex_async = AsyncMock()
        executor._gap_closed = MagicMock(return_value=False)
        executor._compute_zone_quote = MagicMock(return_value=Decimal("10"))
        executor._compute_chunk_size = MagicMock(return_value=(Decimal("1"), Decimal("1"), Decimal("0.01")))
        executor._place_market_order = MagicMock()
        log_mock = MagicMock()
        executor.logger = MagicMock(return_value=log_mock)

        asyncio.run(executor._running_step())

        executor._place_market_order.assert_not_called()
        self.assertEqual(executor._status, RunnableStatus.RUNNING)
        self.assertIn("not widening", executor._skip_reason)
        log_text = " ".join(str(c) for c in log_mock.info.call_args_list)
        self.assertIn("gap_not_widening", log_text)

    def test_running_step_skips_when_zone_quote_exceeds_threshold(self):
        executor = ConvergenceExecutor.__new__(ConvergenceExecutor)
        executor.config = self._config()
        executor.config.require_gap_widening = False
        executor.config.zone_quote_threshold_usdt = Decimal("50")
        executor.config.sweep_chunks = 1
        executor._sweep_complete = False
        executor._order = None
        executor._sweep_side = None
        executor._chunks_remaining = 0
        executor._skip_reason = None
        executor._current_retries = 0
        executor._max_retries = 10
        executor._status = RunnableStatus.RUNNING
        executor._total_filled_base = Decimal("0")
        executor.close_type = None

        dex_feed = MagicMock()
        dex_feed.should_poll.return_value = False
        dex_feed.is_stale.return_value = False
        dex_feed.get_dex_fair.return_value = Decimal("1")
        dex_feed.sanity_ok.return_value = True
        executor._dex_feed = dex_feed

        executor._ensure_dex_feed = MagicMock(return_value=True)
        executor._cex_mid = MagicMock(return_value=Decimal("1.01"))
        executor._poll_dex_async = AsyncMock()
        executor._compute_zone_quote = MagicMock(return_value=Decimal("51"))
        executor._compute_chunk_size = MagicMock(return_value=(Decimal("1"), Decimal("1"), Decimal("0.01")))
        executor._place_market_order = MagicMock()
        executor.stop = MagicMock()
        log_mock = MagicMock()
        executor.logger = MagicMock(return_value=log_mock)

        asyncio.run(executor._running_step())

        self.assertIn("zone quote", executor._skip_reason)
        executor._place_market_order.assert_not_called()
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)
        log_text = " ".join(str(c) for c in log_mock.info.call_args_list)
        self.assertIn("finish_no_trade", log_text)
        self.assertIn("zone_quote_above_threshold", log_text)


class TestConvergenceExecutorConfig(unittest.TestCase):
    def test_config_requires_rpc(self):
        with self.assertRaises(Exception):
            ConvergenceExecutorConfig(
                connector_name="gate_io",
                trading_pair="ALI-USDT",
                dex_rpc_url="",
                dex_pool_address="0xpool",
                dex_base_token_address="0xbase",
                dex_quote_token_address="0xquote",
            )

    def test_config_accepts_env_style_rpc(self):
        with patch.dict("os.environ", {"DEX_RPC_URL": "http://localhost:8545"}):
            cfg = ConvergenceExecutorConfig(
                connector_name="gate_io",
                trading_pair="ALI-USDT",
                dex_pool_address="0xpool",
                dex_base_token_address="0xbase",
                dex_quote_token_address="0xquote",
            )
            self.assertEqual(cfg.dex_rpc_url, "http://localhost:8545")
            self.assertEqual(cfg.zone_quote_threshold_usdt, Decimal("100"))

import asyncio
import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from controllers.generic.convergence_controller import ConvergenceController, ConvergenceControllerConfig
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class TestConvergenceController(unittest.TestCase):
    def _config(self):
        return ConvergenceControllerConfig(
            dex_rpc_url="http://localhost:8545",
            dex_pool_address="0xpool",
            dex_base_token_address="0xbase",
            dex_quote_token_address="0xquote",
            connector_name="gate_io",
            trading_pair="ALI-USDT",
            min_gap_bps=Decimal("30"),
            cooldown_time=0,
        )

    def test_arb_active_detects_running_arb_on_same_cex_pair(self):
        controller = ConvergenceController.__new__(ConvergenceController)
        controller.config = self._config()
        arb_cfg = ArbitrageExecutorConfig(
            buying_market=ConnectorPair(connector_name="gate_io", trading_pair="ALI-USDT"),
            selling_market=ConnectorPair(connector_name="uniswap", trading_pair="ALI-USDT"),
            order_amount=Decimal("10"),
            min_profitability=Decimal("0.001"),
        )
        controller.executors_info = [
            ExecutorInfo(
                id="arb1",
                timestamp=0,
                type="arbitrage_executor",
                status=RunnableStatus.RUNNING,
                config=arb_cfg,
                net_pnl_pct=Decimal("0"),
                net_pnl_quote=Decimal("0"),
                cum_fees_quote=Decimal("0"),
                filled_amount_quote=Decimal("0"),
                is_active=True,
                is_trading=True,
                custom_info={},
            ),
        ]
        self.assertTrue(controller._arb_active_on_pair())

    def test_arb_active_false_when_terminated(self):
        controller = ConvergenceController.__new__(ConvergenceController)
        controller.config = self._config()
        arb_cfg = ArbitrageExecutorConfig(
            buying_market=ConnectorPair(connector_name="gate_io", trading_pair="ALI-USDT"),
            selling_market=ConnectorPair(connector_name="uniswap", trading_pair="ALI-USDT"),
            order_amount=Decimal("10"),
            min_profitability=Decimal("0.001"),
        )
        controller.executors_info = [
            ExecutorInfo(
                id="arb1",
                timestamp=0,
                type="arbitrage_executor",
                status=RunnableStatus.TERMINATED,
                config=arb_cfg,
                net_pnl_pct=Decimal("0"),
                net_pnl_quote=Decimal("0"),
                cum_fees_quote=Decimal("0"),
                filled_amount_quote=Decimal("0"),
                is_active=False,
                is_trading=False,
                custom_info={},
            ),
        ]
        self.assertFalse(controller._arb_active_on_pair())

    def test_update_processed_data_sets_skip_reason_between_polls(self):
        controller = ConvergenceController.__new__(ConvergenceController)
        controller.config = self._config()
        controller.processed_data = {}
        controller._last_skip_reason = "gap 5.0bps below min_gap_bps"
        feed = MagicMock()
        feed.should_poll.return_value = False
        feed.is_stale.return_value = False
        feed.get_dex_fair.return_value = Decimal("0.00150")
        controller._dex_feed = feed
        controller._ensure_dex_feed = MagicMock(return_value=True)
        controller._cex_mid = MagicMock(return_value=Decimal("0.00155"))

        asyncio.run(controller.update_processed_data())

        self.assertEqual(
            controller.processed_data["skip_reason"],
            "gap 5.0bps below min_gap_bps",
        )
        self.assertEqual(controller.processed_data["cex_mid"], 0.00155)

    def test_refresh_dex_fair_if_needed_polls_when_stale(self):
        controller = ConvergenceController.__new__(ConvergenceController)
        controller.config = self._config()
        feed = MagicMock()
        feed.get_dex_fair.return_value = Decimal("0.00150")
        feed.is_stale.return_value = True
        feed.should_poll.return_value = False
        controller._dex_feed = feed

        controller._refresh_dex_fair_if_needed(Decimal("0.00155"))

        feed.poll.assert_called_once_with(cex_mid=Decimal("0.00155"))

    def test_determine_actions_logs_gap_below_min(self):
        controller = ConvergenceController.__new__(ConvergenceController)
        controller.config = self._config()
        controller.config.min_gap_bps = Decimal("30")
        controller.executors_info = []
        controller._last_spawn_timestamp = 0.0
        controller._last_logged_skip_key = None
        controller._last_logged_skip_ts = 0.0
        controller._last_skip_reason = None
        controller._active_convergence_executors = MagicMock(return_value=[])
        controller._arb_active_on_pair = MagicMock(return_value=False)
        controller._ensure_dex_feed = MagicMock(return_value=True)
        controller._cex_mid = MagicMock(return_value=Decimal("0.001501"))
        controller._refresh_dex_fair_if_needed = MagicMock()
        log_mock = MagicMock()
        controller.logger = MagicMock(return_value=log_mock)

        feed = MagicMock()
        feed.is_stale.return_value = False
        feed.get_dex_fair.return_value = Decimal("0.00150")
        feed.sanity_ok.return_value = True
        controller._dex_feed = feed
        controller.market_data_provider = MagicMock()
        controller.market_data_provider.time.return_value = 1000.0

        actions = controller.determine_executor_actions()

        self.assertEqual(actions, [])
        log_text = " ".join(str(c) for c in log_mock.info.call_args_list)
        self.assertIn("Convergence controller skip", log_text)
        self.assertIn("gap_below_min", log_text)

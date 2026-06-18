"""
Instrument refresh cycle: bulk cancel → stop executors → place new orders.

Run:
  pytest test/hummingbot/strategy_v2/controllers/test_market_making_refresh_timing.py -v -s
"""
import asyncio
import inspect
import time
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import PositionMode, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.controllers.market_making_controller_base import (
    MarketMakingControllerBase,
    MarketMakingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

BULK_CANCEL_TIMEOUT_S = 15.0
CONTROLLER_TICK_S = 1.0
EXECUTOR_TICK_S = 1.0
SHUTDOWN_SLEEP_S = 5.0


def _mexc_like_config(**overrides) -> MarketMakingControllerConfigBase:
    """12 levels (6 buy + 6 sell), same shape as conf_pmm_dynamic_dex_cex_mexc.yml."""
    defaults = dict(
        id="refresh_timing_test",
        controller_name="market_making_test",
        connector_name="mexc",
        trading_pair="ALI-USDT",
        total_amount_quote=Decimal("4000"),
        buy_spreads=[0.001, 0.002, 0.004, 0.006, 0.008, 0.01],
        sell_spreads=[0.005, 0.01, 0.012, 0.014, 0.016, 0.018],
        buy_amounts_pct=[Decimal("1")] * 6,
        sell_amounts_pct=[Decimal("1")] * 6,
        executor_refresh_time=120,
        cooldown_time=300,
        cancel_open_orders_on_refresh=True,
        skip_rebalance=True,
        leverage=1,
        position_mode=PositionMode.ONEWAY,
    )
    defaults.update(overrides)
    return MarketMakingControllerConfigBase(**defaults)


def _active_executor_info(
    level_id: str,
    *,
    executor_id: str,
    timestamp: float,
) -> ExecutorInfo:
    side = TradeType.BUY if level_id.startswith("buy") else TradeType.SELL
    return ExecutorInfo(
        id=executor_id,
        timestamp=timestamp,
        type="position_executor",
        status=RunnableStatus.RUNNING,
        config=PositionExecutorConfig(
            id=executor_id,
            timestamp=timestamp,
            connector_name="mexc",
            trading_pair="ALI-USDT",
            side=side,
            entry_price=Decimal("0.0014"),
            amount=Decimal("1000"),
            level_id=level_id,
        ),
        net_pnl_pct=Decimal("0"),
        net_pnl_quote=Decimal("0"),
        cum_fees_quote=Decimal("0"),
        filled_amount_quote=Decimal("0"),
        is_active=True,
        is_trading=False,
        custom_info={"level_id": level_id, "side": side},
        close_timestamp=None,
        close_type=None,
        controller_id="refresh_timing_test",
    )


def _stopped_executor_info(info: ExecutorInfo) -> ExecutorInfo:
    return info.model_copy(
        update={
            "status": RunnableStatus.TERMINATED,
            "is_active": False,
            "is_trading": False,
            "close_type": CloseType.EARLY_STOP,
            "close_timestamp": info.timestamp + 130,
        }
    )


class TestMarketMakingRefreshTiming(IsolatedAsyncioWrapperTestCase):
    """Simulate one refresh cycle and print phase timings."""

    def setUp(self):
        self.config = _mexc_like_config()
        self.mdp = MagicMock(spec=MarketDataProvider)
        self.mdp.ready = True
        self.mdp.time = MagicMock(return_value=1000.0)
        type(self.mdp).get_price_by_type = MagicMock(return_value=Decimal("0.00145"))
        self.actions_queue = asyncio.Queue()
        self.controller = MarketMakingControllerBase(
            config=self.config,
            market_data_provider=self.mdp,
            actions_queue=self.actions_queue,
            update_interval=CONTROLLER_TICK_S,
        )
        self.controller.processed_data = {
            "reference_price": Decimal("0.00145"),
            "spread_multiplier": Decimal("1"),
        }

    async def _run_cancel_phase(self, cancel_delay_s: float) -> float:
        async def slow_cancel(**_kwargs):
            await asyncio.sleep(cancel_delay_s)
            return [CancellationResult("oid-1", True)]

        mock_connector = MagicMock()
        mock_connector.cancel_all_open_orders_for_trading_pair = AsyncMock(side_effect=slow_cancel)
        self.mdp.get_connector = MagicMock(return_value=mock_connector)

        t0 = time.perf_counter()
        with patch.object(
            self.controller,
            "executors_to_refresh",
            return_value=[StopExecutorAction(controller_id=self.config.id, executor_id="e-buy-0")],
        ):
            await self.controller._maybe_cancel_open_orders_before_refresh()
        return time.perf_counter() - t0

    def test_bulk_cancel_timeout_constant(self):
        """Documents exchange cancel cap used on refresh."""
        from hummingbot.strategy_v2.controllers import market_making_controller_base as mm_base

        src = inspect.getsource(mm_base.MarketMakingControllerBase._maybe_cancel_open_orders_before_refresh)
        self.assertIn("timeout_seconds=15.0", src)

    async def test_stop_blocks_create_on_same_controller_tick(self):
        """While executors still is_active in executors_info, refresh sends stops only."""
        now = 1000.0
        self.mdp.time.return_value = now
        level_ids = [f"buy_{i}" for i in range(6)] + [f"sell_{i}" for i in range(6)]
        self.controller.executors_info = [
            _active_executor_info(lid, executor_id=f"e-{lid}", timestamp=now - 130)
            for lid in level_ids
        ]

        actions = self.controller.determine_executor_actions()
        stop_ids = {a.executor_id for a in actions if isinstance(a, StopExecutorAction)}
        create_levels = {a.executor_config.level_id for a in actions if isinstance(a, CreateExecutorAction)}

        self.assertEqual(len(stop_ids), 12)
        self.assertEqual(create_levels, set())

    async def test_simulated_refresh_cycle_reports_phases(self):
        """
        End-to-end simulation of one refresh:
          cancel → stop batch → orchestrator ack → create batch → orders live
        Prints a timing breakdown to stdout (-s).
        """
        cancel_delay_s = 0.05
        orchestrator_delay_s = 0.02
        now = 1000.0
        self.mdp.time.return_value = now
        level_ids = [f"buy_{i}" for i in range(6)] + [f"sell_{i}" for i in range(6)]
        active = [
            _active_executor_info(lid, executor_id=f"e-{lid}", timestamp=now - 130)
            for lid in level_ids
        ]
        self.controller.executors_info = active

        phases = {}
        t_start = time.perf_counter()

        phases["bulk_cancel_s"] = await self._run_cancel_phase(cancel_delay_s)

        t2 = time.perf_counter()
        actions_tick1 = self.controller.determine_executor_actions()
        phases["tick1_stop_only_s"] = time.perf_counter() - t2
        self.assertTrue(all(isinstance(a, StopExecutorAction) for a in actions_tick1))

        await self.actions_queue.put(actions_tick1)
        self.controller.executors_update_event.clear()

        async def mock_orchestrator():
            await asyncio.sleep(orchestrator_delay_s)
            self.controller.executors_info = [_stopped_executor_info(e) for e in active]

        t3 = time.perf_counter()
        await mock_orchestrator()
        self.controller.executors_update_event.set()
        phases["orchestrator_stop_ack_s"] = time.perf_counter() - t3

        t4 = time.perf_counter()
        with patch.object(
            self.controller,
            "get_executor_config",
            side_effect=lambda level_id, price, amount: PositionExecutorConfig(
                id=f"new-{level_id}",
                timestamp=now,
                connector_name="mexc",
                trading_pair="ALI-USDT",
                side=TradeType.BUY if level_id.startswith("buy") else TradeType.SELL,
                entry_price=price or Decimal("0.0014"),
                amount=amount or Decimal("1000"),
                level_id=level_id,
            ),
        ):
            with patch.object(
                self.controller,
                "get_price_and_amount",
                return_value=(Decimal("0.0014"), Decimal("1000")),
            ):
                actions_tick2 = self.controller.determine_executor_actions()
        phases["tick2_create_proposal_s"] = time.perf_counter() - t4

        create_actions = [a for a in actions_tick2 if isinstance(a, CreateExecutorAction)]
        self.assertEqual(len(create_actions), 12)

        t5 = time.perf_counter()
        await asyncio.sleep(EXECUTOR_TICK_S)
        phases["executor_first_order_tick_s"] = time.perf_counter() - t5

        phases["total_book_empty_estimate_s"] = (
            phases["bulk_cancel_s"]
            + CONTROLLER_TICK_S
            + phases["orchestrator_stop_ack_s"]
            + CONTROLLER_TICK_S
            + phases["executor_first_order_tick_s"]
        )
        phases["wall_clock_s"] = time.perf_counter() - t_start

        print("\n=== Refresh cycle timing (simulated) ===")
        for k, v in phases.items():
            print(f"  {k}: {v:.3f}s")
        print(f"  (theoretical min with instant cancel: ~{2 * CONTROLLER_TICK_S + EXECUTOR_TICK_S:.1f}s)")
        print(
            f"  (theoretical max cancel-bound: "
            f"~{BULK_CANCEL_TIMEOUT_S + 2 * CONTROLLER_TICK_S + EXECUTOR_TICK_S:.1f}s)"
        )

    @patch(
        "hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor._sleep",
        new_callable=AsyncMock,
    )
    async def test_position_executor_shutdown_sleeps_5s_per_loop(self, mock_sleep: AsyncMock):
        """Shutdown path calls _sleep(5.0) each control_task while winding down."""
        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).current_timestamp = PropertyMock(return_value=1234567890)
        strategy.cancel = MagicMock(return_value=None)
        strategy.connectors = {"mexc": MagicMock()}

        cfg = PositionExecutorConfig(
            id="shutdown-test",
            timestamp=1234567890,
            connector_name="mexc",
            trading_pair="ALI-USDT",
            side=TradeType.BUY,
            entry_price=Decimal("0.0014"),
            amount=Decimal("1000"),
            level_id="buy_0",
        )
        ex = PositionExecutor(strategy, cfg, update_interval=EXECUTOR_TICK_S)
        ex._status = RunnableStatus.SHUTTING_DOWN
        ex.close_type = CloseType.EARLY_STOP

        await ex.control_task()
        mock_sleep.assert_awaited_with(SHUTDOWN_SLEEP_S)

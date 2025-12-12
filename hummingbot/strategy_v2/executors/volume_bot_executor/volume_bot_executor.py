import asyncio
import logging
import random
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import PositionAction, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.volume_bot_executor.data_types import (
    VolumeBotExecutorConfig,
    VolumeBotMode,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class VolumeBotState:
    """Internal state tracking for Volume Bot"""
    def __init__(self):
        self.current_cycle: int = 0
        self.pending_buys: int = 0
        self.pending_sells: int = 0
        self.total_buys_executed: int = 0
        self.total_sells_executed: int = 0
        self.last_execution_time: float = 0.0
        self.accumulated_base_token: Decimal = Decimal("0")
        self.accumulated_quote_token: Decimal = Decimal("0")
        self.current_batch_buys: List[TrackedOrder] = []
        self.current_batch_sells: List[TrackedOrder] = []
        self.failed_orders: List[TrackedOrder] = []
        self.is_executing_buys: bool = False
        self.is_executing_sells: bool = False
        self.next_buy_time: float = 0.0
        self.next_sell_time: float = 0.0
        self.next_cycle_time: float = 0.0


class VolumeBotExecutor(ExecutorBase):
    """
    Volume Bot Executor - Generates trading volume through balanced buy/sell operations
    
    Architecture: Follows same executor pattern as TWAPExecutor
    - Extends ExecutorBase
    - Uses same event handling system
    - Follows same lifecycle (RUNNING → SHUTTING_DOWN → TERMINATED)
    - Reuses same error handling and retry mechanisms
    """
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy: ScriptStrategyBase,
        config: VolumeBotExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 15,
    ):
        """
        Initialize Volume Bot Executor
        
        Follows same initialization pattern as TWAPExecutor
        """
        super().__init__(
            strategy=strategy,
            connectors=[config.connector_name],
            config=config,
            update_interval=update_interval,
        )
        self.config = config
        self._max_retries = max_retries
        self._current_retries = 0
        self._start_timestamp = self._strategy.current_timestamp
        
        # Initialize state (similar to TWAP's order_plan)
        self._state = VolumeBotState()
        self._state.last_execution_time = self._start_timestamp
        
        # Validate configuration
        trading_rules = self.get_trading_rules(config.connector_name, config.trading_pair)
        if self.config.trade_amount_quote < trading_rules.min_order_size:
            self.close_execution_by(CloseType.FAILED)
            self.logger().error(
                f"Trade amount {self.config.trade_amount_quote} is less than "
                f"minimum order size {trading_rules.min_order_size}"
            )
        
        if self.config.is_maker:
            self.logger().warning("Maker mode (limit orders) is in beta. Please use with caution.")
        
        # Initialize cycle timing
        self._initialize_cycle_timing()

    def _initialize_cycle_timing(self):
        """Initialize timing for first cycle"""
        self._state.next_buy_time = self._strategy.current_timestamp
        self._state.next_cycle_time = self._strategy.current_timestamp

    def close_execution_by(self, close_type: CloseType):
        """Close executor with specified close type - same as TWAP"""
        self.close_type = close_type
        self.close_timestamp = self._strategy.current_timestamp
        self.stop()

    async def validate_sufficient_balance(self):
        """
        Validate sufficient balance for both buy and sell operations
        Similar to TWAP but checks both directions
        """
        mid_price = self.get_price(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice
        )
        
        # Calculate amounts needed
        total_buy_amount_base = self.config.trade_amount_quote * self.config.buy_batch_size / mid_price
        total_sell_amount_base = self.config.trade_amount_quote * self.config.sell_batch_size / mid_price
        
        # Check buy balance (needs quote currency)
        if self.is_perpetual_connector(self.config.connector_name):
            buy_candidate = PerpetualOrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.is_maker,
                order_type=self.config.order_type,
                order_side=TradeType.BUY,
                amount=total_buy_amount_base,
                price=mid_price,
                leverage=Decimal(self.config.leverage),
            )
        else:
            buy_candidate = OrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.is_maker,
                order_type=self.config.order_type,
                order_side=TradeType.BUY,
                amount=total_buy_amount_base,
                price=mid_price,
            )
        
        adjusted_buy = self.adjust_order_candidates(self.config.connector_name, [buy_candidate])
        if adjusted_buy[0].amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough balance for buy operations.")
            self.stop()
            return
        
        # Check sell balance (needs base currency)
        if self.is_perpetual_connector(self.config.connector_name):
            sell_candidate = PerpetualOrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.is_maker,
                order_type=self.config.order_type,
                order_side=TradeType.SELL,
                amount=total_sell_amount_base,
                price=mid_price,
                leverage=Decimal(self.config.leverage),
            )
        else:
            sell_candidate = OrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.is_maker,
                order_type=self.config.order_type,
                order_side=TradeType.SELL,
                amount=total_sell_amount_base,
                price=mid_price,
            )
        
        adjusted_sell = self.adjust_order_candidates(self.config.connector_name, [sell_candidate])
        if adjusted_sell[0].amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough balance for sell operations.")
            self.stop()

    async def control_task(self):
        """
        Main control loop - follows same pattern as TWAPExecutor
        
        Execution flow:
        1. Check if time to execute buys
        2. Execute buy batch
        3. Wait for cycle interval
        4. Execute sell batch
        5. Verify balance
        6. Repeat
        """
        if self.status == RunnableStatus.RUNNING:
            self._evaluate_execute_buys()
            self._evaluate_execute_sells()
            self._evaluate_balance_verification()
            self._evaluate_max_retries()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self._evaluate_all_orders_closed()

    def _evaluate_execute_buys(self):
        """Evaluate and execute buy orders when timing is right"""
        current_time = self._strategy.current_timestamp
        
        # Check if it's time to start a new cycle
        if (
            not self._state.is_executing_buys
            and not self._state.is_executing_sells
            and current_time >= self._state.next_cycle_time
        ):
            self._start_buy_cycle()
        
        # Execute individual buy orders within batch
        if self._state.is_executing_buys and current_time >= self._state.next_buy_time:
            self._execute_single_buy()
            self._state.next_buy_time = current_time + self.config.interval_between_buys
            
            # Check if batch complete
            if len(self._state.current_batch_buys) >= self._get_current_buy_batch_size():
                self._complete_buy_batch()

    def _evaluate_execute_sells(self):
        """Evaluate and execute sell orders when timing is right"""
        current_time = self._strategy.current_timestamp
        
        # Start sell cycle after buy cycle completes
        if (
            not self._state.is_executing_buys
            and not self._state.is_executing_sells
            and self._state.pending_buys > 0
            and current_time >= self._state.next_cycle_time
        ):
            self._start_sell_cycle()
        
        # Execute individual sell orders within batch
        if self._state.is_executing_sells and current_time >= self._state.next_sell_time:
            self._execute_single_sell()
            self._state.next_sell_time = current_time + self.config.interval_between_sells
            
            # Check if batch complete
            if len(self._state.current_batch_sells) >= self._get_current_sell_batch_size():
                self._complete_sell_batch()

    def _start_buy_cycle(self):
        """Start a new buy cycle"""
        self._state.current_cycle += 1
        self._state.is_executing_buys = True
        self._state.current_batch_buys = []
        self._state.next_buy_time = self._strategy.current_timestamp
        self.logger().info(f"Starting buy cycle {self._state.current_cycle}")

    def _start_sell_cycle(self):
        """Start a new sell cycle"""
        self._state.is_executing_sells = True
        self._state.current_batch_sells = []
        self._state.next_sell_time = self._strategy.current_timestamp
        self.logger().info(f"Starting sell cycle {self._state.current_cycle}")

    def _execute_single_buy(self):
        """Execute a single buy order"""
        price = self.get_price(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice
        )
        amount = self.config.trade_amount_quote / price
        
        # Apply price buffer for limit orders
        if self.config.is_maker and self.config.limit_order_buffer:
            order_price = price * (Decimal("1") - self.config.limit_order_buffer)
        else:
            order_price = price
        
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=self.config.order_type,
            side=TradeType.BUY,
            amount=amount,
            price=order_price,
            position_action=PositionAction.OPEN,
        )
        
        tracked_order = TrackedOrder(order_id=order_id)
        self._state.current_batch_buys.append(tracked_order)
        self._state.total_buys_executed += 1
        self._state.pending_buys += 1
        
        self.logger().info(
            f"Placed buy order {order_id} - Amount: {amount}, Price: {order_price}"
        )

    def _execute_single_sell(self):
        """Execute a single sell order"""
        price = self.get_price(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice
        )
        amount = self.config.trade_amount_quote / price
        
        # Apply price buffer for limit orders
        if self.config.is_maker and self.config.limit_order_buffer:
            order_price = price * (Decimal("1") + self.config.limit_order_buffer)
        else:
            order_price = price
        
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=self.config.order_type,
            side=TradeType.SELL,
            amount=amount,
            price=order_price,
            position_action=PositionAction.OPEN,
        )
        
        tracked_order = TrackedOrder(order_id=order_id)
        self._state.current_batch_sells.append(tracked_order)
        self._state.total_sells_executed += 1
        self._state.pending_sells += 1
        
        self.logger().info(
            f"Placed sell order {order_id} - Amount: {amount}, Price: {order_price}"
        )

    def _complete_buy_batch(self):
        """Complete buy batch and prepare for sell cycle"""
        self._state.is_executing_buys = False
        self._state.next_cycle_time = (
            self._strategy.current_timestamp + self.config.interval_between_cycles
        )
        self.logger().info(
            f"Completed buy batch - {len(self._state.current_batch_buys)} orders placed"
        )

    def _complete_sell_batch(self):
        """Complete sell batch and verify balance"""
        self._state.is_executing_sells = False
        self._state.next_cycle_time = (
            self._strategy.current_timestamp + self.config.interval_between_cycles
        )
        self.logger().info(
            f"Completed sell batch - {len(self._state.current_batch_sells)} orders placed"
        )
        self._verify_balance()

    def _verify_balance(self):
        """Verify that buy/sell balance is maintained"""
        net_position = self._state.pending_buys - self._state.pending_sells
        
        if abs(net_position) > 1:  # Allow 1 order difference for tolerance
            self.logger().warning(
                f"Balance warning - Pending buys: {self._state.pending_buys}, "
                f"Pending sells: {self._state.pending_sells}, Net: {net_position}"
            )
        else:
            self.logger().info(
                f"Balance maintained - Buys: {self._state.total_buys_executed}, "
                f"Sells: {self._state.total_sells_executed}"
            )
        
        # Reset pending counts after verification
        self._state.pending_buys = 0
        self._state.pending_sells = 0

    def _get_current_buy_batch_size(self) -> int:
        """Get current buy batch size based on mode"""
        if self.config.mode == VolumeBotMode.VARIABLE:
            if self.config.randomize:
                return random.randint(
                    self.config.min_batch_size, self.config.max_batch_size
                )
            return self.config.buy_batch_size
        return self.config.buy_batch_size

    def _get_current_sell_batch_size(self) -> int:
        """Get current sell batch size based on mode"""
        if self.config.mode == VolumeBotMode.VARIABLE:
            if self.config.randomize:
                return random.randint(
                    self.config.min_batch_size, self.config.max_batch_size
                )
            return self.config.sell_batch_size
        return self.config.sell_batch_size

    def _evaluate_balance_verification(self):
        """Periodically verify overall balance"""
        # This can be extended for more sophisticated balance checking
        pass

    def _evaluate_max_retries(self):
        """Check retry limits - same as TWAP"""
        if self._current_retries > self._max_retries:
            self.close_execution_by(CloseType.FAILED)

    async def _evaluate_all_orders_closed(self):
        """Wait for all orders to close - same pattern as TWAP"""
        all_orders = (
            self._state.current_batch_buys
            + self._state.current_batch_sells
            + self._state.failed_orders
        )
        
        all_done = all(
            order.is_done
            for order in all_orders
            if order and order.order
        )
        
        if all_done:
            self.close_execution_by(CloseType.COMPLETED)
            self._status = RunnableStatus.TERMINATED
        else:
            await asyncio.sleep(5)

    # Event Handlers - Same pattern as TWAPExecutor
    def process_order_created_event(
        self,
        event_tag: int,
        market: ConnectorBase,
        event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent],
    ):
        """Process order created event - same as TWAP"""
        self._update_tracked_orders_with_order_id(event.order_id)

    def process_order_failed_event(
        self,
        event_tag: int,
        market: ConnectorBase,
        event: MarketOrderFailureEvent,
    ):
        """Process order failed event - same as TWAP"""
        # Find failed order in current batches
        for order_list in [
            self._state.current_batch_buys,
            self._state.current_batch_sells,
        ]:
            failed_order = next(
                (order for order in order_list if order.order_id == event.order_id),
                None,
            )
            if failed_order:
                self._state.failed_orders.append(failed_order)
                order_list.remove(failed_order)
                self._current_retries += 1
                self.logger().warning(f"Order {event.order_id} failed, retry count: {self._current_retries}")

    def process_order_completed_event(
        self,
        event_tag: int,
        market: ConnectorBase,
        event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent],
    ):
        """Process order completed event"""
        self._update_tracked_orders_with_order_id(event.order_id)

    def _update_tracked_orders_with_order_id(self, order_id: str):
        """Update tracked order with InFlightOrder - same as TWAP"""
        all_orders = (
            self._state.current_batch_buys
            + self._state.current_batch_sells
            + self._state.failed_orders
        )
        
        active_order = next(
            (order for order in all_orders if order.order_id == order_id), None
        )
        
        if active_order:
            in_flight_order = self.get_in_flight_order(
                self.config.connector_name, order_id
            )
            if in_flight_order:
                active_order.order = in_flight_order

    def cancel_open_orders(self):
        """Cancel all open orders - same as TWAP"""
        all_orders = (
            self._state.current_batch_buys
            + self._state.current_batch_sells
        )
        
        for order in all_orders:
            if order and order.order and order.order.is_open:
                self._strategy.cancel(
                    self.config.connector_name,
                    self.config.trading_pair,
                    order.order_id,
                )

    def early_stop(self, keep_position: bool = False):
        """Early stop executor - same as TWAP"""
        self.close_execution_by(CloseType.EARLY_STOP)
        self.cancel_open_orders()
        self._status = RunnableStatus.SHUTTING_DOWN
        self.logger().info("Volume Bot executor stopped early.")

    # Performance Metrics - Similar to TWAP
    @property
    def total_volume_quote(self) -> Decimal:
        """Total volume generated in quote currency"""
        return Decimal(self._state.total_buys_executed + self._state.total_sells_executed) * self.config.trade_amount_quote

    @property
    def net_position(self) -> int:
        """Net position (buys - sells)"""
        return self._state.total_buys_executed - self._state.total_sells_executed

    @property
    def balance_ratio(self) -> float:
        """Balance ratio (sells/buys)"""
        if self._state.total_buys_executed == 0:
            return 0.0
        return self._state.total_sells_executed / self._state.total_buys_executed

    def get_cum_fees_quote(self) -> Decimal:
        """Calculate cumulative fees - same pattern as TWAP"""
        all_orders = (
            self._state.current_batch_buys
            + self._state.current_batch_sells
            + self._state.failed_orders
        )
        return sum(
            order.cum_fees_quote
            for order in all_orders
            if order and hasattr(order, "cum_fees_quote")
        )

    def get_net_pnl_quote(self) -> Decimal:
        """Net PnL calculation"""
        # For volume bot, PnL should be minimal due to balanced trading
        return Decimal("0")  # Implement if needed

    def get_net_pnl_pct(self) -> Decimal:
        """Net PnL percentage"""
        return Decimal("0")  # Implement if needed


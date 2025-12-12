# TWAP Strategy: Detailed Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [Class Hierarchy](#class-hierarchy)
3. [Component Architecture](#component-architecture)
4. [Data Flow](#data-flow)
5. [Execution Flow](#execution-flow)
6. [State Management](#state-management)
7. [Event Handling System](#event-handling-system)
8. [Key Methods Deep Dive](#key-methods-deep-dive)
9. [Order Management](#order-management)
10. [Code Walkthrough](#code-walkthrough)

---

## Overview

The TWAP (Time-Weighted Average Price) strategy is implemented as an **executor** in Hummingbot's Strategy V2 framework. It inherits from `ExecutorBase` and implements time-weighted order execution logic.

### Core Concept
- **Splits large orders** into smaller orders placed at regular time intervals
- **Reduces market impact** by distributing execution over time
- **Achieves average price** through time-weighted execution

---

## Class Hierarchy

```
RunnableBase (Abstract)
    │
    └── ExecutorBase (Abstract)
            │
            └── TWAPExecutor (Concrete Implementation)
```

### Inheritance Chain

**1. RunnableBase** (Base Runnable Class)
- Provides async task execution framework
- Manages status: `RUNNING`, `SHUTTING_DOWN`, `TERMINATED`
- Handles update intervals

**2. ExecutorBase** (Base Executor Class)
```python
class ExecutorBase(RunnableBase):
    - Manages connector access
    - Provides order placement methods
    - Handles event forwarding
    - Provides price and trading rules access
```

**3. TWAPExecutor** (TWAP Implementation)
```python
class TWAPExecutor(ExecutorBase):
    - Implements TWAP-specific logic
    - Manages order plan (time-based schedule)
    - Handles dynamic amount calculation
    - Manages MAKER mode order refresh
```

---

## Component Architecture

### 1. Configuration Layer (`TWAPExecutorConfig`)

**Location**: `hummingbot/strategy_v2/executors/twap_executor/data_types.py`

```python
class TWAPExecutorConfig(ExecutorConfigBase):
    # Required Parameters
    type: Literal["twap_executor"] = "twap_executor"
    connector_name: str                    # Exchange connector
    trading_pair: str                      # Trading pair (e.g., "BTC-USDT")
    side: TradeType                        # BUY or SELL
    total_amount_quote: Decimal            # Total order size in quote currency
    total_duration: int                    # Duration in seconds
    order_interval: int                    # Interval between orders (seconds)
    
    # Optional Parameters
    leverage: int = 1                      # Leverage (for perpetuals)
    mode: TWAPMode = TWAPMode.TAKER        # TAKER or MAKER
    
    # MAKER Mode Parameters
    limit_order_buffer: Optional[Decimal]  # Price offset for limit orders
    order_resubmission_time: Optional[int]  # Resubmit time for unfilled orders
    
    # Computed Properties
    @property
    def number_of_orders(self) -> int:
        return (self.total_duration // self.order_interval) + 1
    
    @property
    def order_amount_quote(self) -> Decimal:
        return self.total_amount_quote / self.number_of_orders
    
    @property
    def order_type(self) -> OrderType:
        return OrderType.LIMIT if self.is_maker else OrderType.MARKET
```

**Key Design Decisions**:
- Uses Pydantic for validation
- Computed properties for derived values
- Enum for mode selection (type safety)

---

### 2. Execution Layer (`TWAPExecutor`)

**Location**: `hummingbot/strategy_v2/executors/twap_executor/twap_executor.py`

#### Core Data Structures

```python
class TWAPExecutor(ExecutorBase):
    def __init__(self, ...):
        # Configuration
        self.config: TWAPExecutorConfig
        
        # Order Planning
        self._order_plan: Dict[float, Optional[TrackedOrder]]
        # Key: timestamp (float)
        # Value: TrackedOrder or None (if not yet created)
        
        # Error Handling
        self._failed_orders: List[TrackedOrder]
        self._refreshed_orders: List[TrackedOrder]
        
        # Retry Management
        self._max_retries: int = 15
        self._current_retries: int = 0
        
        # Timing
        self._start_timestamp: float
```

**Order Plan Structure**:
```python
# Example: 60 second duration, 15 second interval
{
    0.0: None,           # Order at start (not yet created)
    15.0: TrackedOrder,  # Order at 15 seconds (created)
    30.0: None,          # Order at 30 seconds (not yet created)
    45.0: TrackedOrder,  # Order at 45 seconds (created)
    60.0: None           # Order at 60 seconds (not yet created)
}
```

---

## Data Flow

### Initialization Flow

```
1. Strategy creates TWAPExecutorConfig
   ↓
2. Strategy calls: CreateExecutorAction(executor_config=config)
   ↓
3. ExecutorOrchestrator creates TWAPExecutor instance
   ↓
4. TWAPExecutor.__init__():
   - Validates minimum order size
   - Creates order plan (timestamps)
   - Initializes tracking structures
   ↓
5. Executor starts running (status = RUNNING)
   ↓
6. control_task() begins async execution
```

### Order Creation Flow

```
control_task() (every update_interval)
   ↓
evaluate_create_order()
   ↓
For each timestamp in order_plan:
   - Check if current_time >= timestamp
   - Check if order not yet created (tracked_order is None)
   ↓
create_order(timestamp)
   ↓
Calculate dynamic amount:
   - Get current mid price
   - Calculate total executed amount
   - Calculate open orders amount
   - Calculate remaining amount
   - Divide by remaining orders
   ↓
Determine order price:
   - TAKER mode: Use mid price (market order)
   - MAKER mode: Apply buffer (limit order)
   ↓
place_order() → Returns order_id
   ↓
Store in order_plan[timestamp] = TrackedOrder(order_id)
```

### Event Processing Flow

```
Order Created Event
   ↓
process_order_created_event()
   ↓
update_tracked_orders_with_order_id()
   ↓
Retrieve InFlightOrder from connector
   ↓
Store in TrackedOrder.order

Order Completed Event
   ↓
process_order_completed_event()
   ↓
evaluate_all_orders_completed()
   ↓
Check if all orders filled
   ↓
If yes: status = SHUTTING_DOWN

Order Failed Event
   ↓
process_order_failed_event()
   ↓
Move to _failed_orders list
   ↓
Reset order_plan entry to None
   ↓
Increment _current_retries
```

---

## Execution Flow

### Main Control Loop

```python
async def control_task(self):
    """
    Main execution loop - runs continuously
    """
    if self.status == RunnableStatus.RUNNING:
        # 1. Check if it's time to create new orders
        self.evaluate_create_order()
        
        # 2. Check if MAKER orders need refresh
        self.evaluate_refresh_orders()
        
        # 3. Check if all orders completed
        self.evaluate_all_orders_completed()
        
        # 4. Check retry limits
        self.evaluate_max_retries()
        
    elif self.status == RunnableStatus.SHUTTING_DOWN:
        # Wait for all orders to close
        await self.evaluate_all_orders_closed()
```

### Order Creation Logic

```python
def evaluate_create_order(self):
    """
    Checks order plan and creates orders when timestamp is reached
    """
    for timestamp, tracked_order in self._order_plan.items():
        # Condition 1: Current time has passed the scheduled timestamp
        # Condition 2: Order not yet created (None)
        if self._strategy.current_timestamp >= timestamp and tracked_order is None:
            self.create_order(timestamp)
```

### Dynamic Amount Calculation

```python
def create_order(self, timestamp):
    # Step 1: Get current market price
    price = self.get_price(..., PriceType.MidPrice)
    
    # Step 2: Calculate total executed amount (already filled)
    total_executed_amount = self.get_total_executed_amount_quote()
    
    # Step 3: Calculate open orders amount (pending but not filled)
    open_orders_open_amount = sum([
        order.order.amount * order.order.price 
        for order in self._order_plan.values() 
        if order and order.order and not order.is_done
    ])
    
    # Step 4: Calculate remaining amount
    orders_amount_quote_left = (
        self.config.total_amount_quote 
        - total_executed_amount 
        - open_orders_open_amount
    )
    
    # Step 5: Calculate remaining orders count
    number_or_orders_left = (
        self.config.number_of_orders 
        - len([order for order in self._order_plan.values() if order])
    )
    
    # Step 6: Calculate amount for this order
    amount = (orders_amount_quote_left / number_or_orders_left) / price
```

**Why Dynamic?**
- Adjusts for partial fills
- Accounts for failed orders
- Ensures total amount is distributed correctly
- Handles price changes during execution

---

## State Management

### Executor States

```python
class RunnableStatus(Enum):
    RUNNING = "RUNNING"           # Active execution
    SHUTTING_DOWN = "SHUTTING_DOWN"  # Waiting for orders to close
    TERMINATED = "TERMINATED"      # Fully stopped
```

### State Transitions

```
INITIALIZATION
    ↓
RUNNING
    ├──→ SHUTTING_DOWN (all orders filled)
    ├──→ SHUTTING_DOWN (early_stop called)
    └──→ SHUTTING_DOWN (max retries exceeded)
         ↓
    TERMINATED
```

### Order States (TrackedOrder)

```python
class TrackedOrder:
    order_id: str
    order: Optional[InFlightOrder]  # None until order created
    
    # InFlightOrder states:
    # - PENDING_CREATE
    # - OPEN
    # - PARTIALLY_FILLED
    # - FILLED
    # - CANCELED
    # - FAILED
```

---

## Event Handling System

### Event Forwarders (from ExecutorBase)

```python
# Event forwarders are registered in ExecutorBase.__init__()
self._create_buy_order_forwarder = SourceInfoEventForwarder(
    self.process_order_created_event
)
self._create_sell_order_forwarder = SourceInfoEventForwarder(
    self.process_order_created_event
)
self._complete_buy_order_forwarder = SourceInfoEventForwarder(
    self.process_order_completed_event
)
self._complete_sell_order_forwarder = SourceInfoEventForwarder(
    self.process_order_completed_event
)
self._failed_order_forwarder = SourceInfoEventForwarder(
    self.process_order_failed_event
)
```

### Event Flow

```
Exchange/Connector
    ↓ (emits events)
Event Forwarder
    ↓ (routes to executor)
TWAPExecutor Event Handler
    ↓ (processes event)
Update Internal State
```

### Event Handlers

**1. Order Created Event**
```python
def process_order_created_event(self, event):
    """
    Called when order is successfully created on exchange
    """
    # Update TrackedOrder with InFlightOrder object
    self.update_tracked_orders_with_order_id(event.order_id)
```

**2. Order Completed Event**
```python
def process_order_completed_event(self, event):
    """
    Called when order is fully filled
    """
    # Check if all orders are completed
    self.evaluate_all_orders_completed()
```

**3. Order Failed Event**
```python
def process_order_failed_event(self, event):
    """
    Called when order fails to create or is rejected
    """
    # Move to failed orders list
    # Reset order plan entry
    # Increment retry counter
```

---

## Key Methods Deep Dive

### 1. `create_order_plan()`

```python
def create_order_plan(self):
    """
    Creates a schedule of order timestamps
    
    Example: 60s duration, 15s interval
    Returns: {0.0: None, 15.0: None, 30.0: None, 45.0: None, 60.0: None}
    """
    order_plan = {}
    for i in range(self.config.number_of_orders):
        timestamp = self._start_timestamp + i * self.config.order_interval
        order_plan[timestamp] = None
    return order_plan
```

**Purpose**: Pre-calculate all order timestamps at initialization

---

### 2. `create_order(timestamp)`

```python
def create_order(self, timestamp):
    """
    Creates a single order at the specified timestamp
    """
    # 1. Get current market price
    price = self.get_price(..., PriceType.MidPrice)
    
    # 2. Calculate dynamic amount (accounts for fills and failures)
    # ... (see Dynamic Amount Calculation above)
    
    # 3. Determine order price based on mode
    if self.config.is_maker:
        # MAKER: Apply buffer
        order_price = price * (1 + buffer) if SELL else price * (1 - buffer)
    else:
        # TAKER: Use mid price (market order)
        order_price = price
    
    # 4. Place order via ExecutorBase
    order_id = self.place_order(
        connector_name=self.config.connector_name,
        trading_pair=self.config.trading_pair,
        order_type=self.config.order_type,  # MARKET or LIMIT
        side=self.config.side,              # BUY or SELL
        amount=amount,
        price=order_price,
        position_action=PositionAction.OPEN
    )
    
    # 5. Store in order plan
    self._order_plan[timestamp] = TrackedOrder(order_id=order_id)
```

---

### 3. `evaluate_refresh_orders()` (MAKER Mode Only)

```python
def evaluate_refresh_orders(self):
    """
    Refreshes unfilled limit orders in MAKER mode
    """
    if self.config.is_maker:
        for timestamp, tracked_order in self._order_plan.items():
            if self.refresh_order_condition(tracked_order):
                # Cancel old order
                self._strategy.cancel(..., tracked_order.order_id)
                
                # Track as refreshed
                self._refreshed_orders.append(tracked_order)
                
                # Create new order
                self.create_order(timestamp)
```

**Refresh Condition**:
```python
def refresh_order_condition(self, tracked_order):
    """
    Returns True if order should be refreshed
    """
    return (
        tracked_order and 
        tracked_order.order and 
        tracked_order.order.is_open and
        tracked_order.order.creation_timestamp < 
        (current_time - order_resubmission_time)
    )
```

**Purpose**: Keep limit orders competitive by refreshing stale orders

---

### 4. `evaluate_all_orders_completed()`

```python
def evaluate_all_orders_completed(self):
    """
    Checks if all orders are filled and transitions to SHUTTING_DOWN
    """
    if self.evaluate_all_orders_created():
        if all([
            order.order.is_filled 
            for order in self._order_plan.values() 
            if order and order.order
        ]):
            self._status = RunnableStatus.SHUTTING_DOWN
```

**Purpose**: Detect completion and initiate shutdown

---

### 5. `evaluate_all_orders_closed()`

```python
async def evaluate_all_orders_closed(self):
    """
    Waits for all orders (including refreshed/failed) to close
    """
    refreshed_orders_done = all([
        order.is_done for order in self._refreshed_orders
    ])
    failed_orders_done = all([
        order.is_done for order in self._failed_orders
    ])
    
    if refreshed_orders_done and failed_orders_done:
        self.close_execution_by(CloseType.COMPLETED)
        self._status = RunnableStatus.TERMINATED
    else:
        # Wait and retry
        await asyncio.sleep(5)
```

**Purpose**: Ensure all orders are fully closed before termination

---

## Order Management

### Order Lifecycle

```
1. SCHEDULED
   order_plan[timestamp] = None
   
2. CREATING
   create_order() called
   place_order() invoked
   
3. PENDING
   order_id returned
   order_plan[timestamp] = TrackedOrder(order_id)
   
4. CREATED
   process_order_created_event()
   TrackedOrder.order = InFlightOrder
   
5. FILLING
   order.is_filled = False
   order.executed_amount_base > 0
   
6. FILLED
   process_order_completed_event()
   order.is_filled = True
   
7. DONE
   evaluate_all_orders_closed()
   Executor terminated
```

### Failed Order Handling

```python
def process_order_failed_event(self, event):
    """
    Handles order failures
    """
    # Find the failed order in order plan
    active_order = next(
        (order for order in self._order_plan.values() 
         if order.order_id == event.order_id), 
        None
    )
    
    if active_order:
        # Move to failed orders list
        self._failed_orders.append(active_order)
        
        # Reset order plan entry (allows retry)
        self._order_plan = {
            timestamp: None if order == active_order else order
            for timestamp, order in self._order_plan.items()
        }
        
        # Increment retry counter
        self._current_retries += 1
```

**Retry Logic**:
- Failed orders reset order plan entry to `None`
- Next `evaluate_create_order()` cycle will retry
- Max retries: 15 (default)

---

## Code Walkthrough

### Complete Execution Example

**Configuration**:
```python
config = TWAPExecutorConfig(
    connector_name="binance",
    trading_pair="BTC-USDT",
    side=TradeType.BUY,
    total_amount_quote=Decimal("1000"),
    total_duration=60,      # 1 minute
    order_interval=15,       # 15 seconds
    mode=TWAPMode.TAKER
)
```

**Execution Timeline**:

```
t=0s:   Executor initialized
        order_plan = {0: None, 15: None, 30: None, 45: None, 60: None}
        
t=0s:   evaluate_create_order() → create_order(0)
        - Calculate amount: 1000 / 5 = 200 USDT
        - Place MARKET order for ~200 USDT worth of BTC
        - order_plan[0] = TrackedOrder(order_id="abc123")
        
t=1s:   process_order_created_event()
        - TrackedOrder.order = InFlightOrder
        
t=2s:   Order filled
        - process_order_completed_event()
        - TrackedOrder.order.is_filled = True
        
t=15s:  evaluate_create_order() → create_order(15)
        - Calculate amount: (1000 - 200) / 4 = 200 USDT
        - Place MARKET order
        - order_plan[15] = TrackedOrder(order_id="def456")
        
t=30s:  evaluate_create_order() → create_order(30)
        - Calculate amount: (1000 - 400) / 3 = 200 USDT
        - Place MARKET order
        
t=45s:  evaluate_create_order() → create_order(45)
        - Calculate amount: (1000 - 600) / 2 = 200 USDT
        - Place MARKET order
        
t=60s:  evaluate_create_order() → create_order(60)
        - Calculate amount: (1000 - 800) / 1 = 200 USDT
        - Place MARKET order
        
t=65s:  All orders filled
        - evaluate_all_orders_completed()
        - status = SHUTTING_DOWN
        
t=66s:  evaluate_all_orders_closed()
        - All orders done
        - status = TERMINATED
```

---

## Integration Points

### 1. Strategy Integration

```python
class MyStrategy(StrategyV2Base):
    def determine_executor_actions(self):
        actions = []
        
        # Create TWAP executor
        twap_config = TWAPExecutorConfig(...)
        actions.append(CreateExecutorAction(executor_config=twap_config))
        
        return actions
```

### 2. ExecutorBase Integration

**Methods Used from ExecutorBase**:
- `place_order()`: Places orders via strategy
- `get_price()`: Gets market price
- `get_trading_rules()`: Gets exchange rules
- `adjust_order_candidates()`: Validates balance

### 3. Connector Integration

**Via ExecutorBase**:
- Access to exchange connector
- Order placement
- Price feeds
- Order book data

---

## Performance Metrics

### Tracking Methods

```python
@property
def filled_amount_quote(self) -> Decimal:
    """Total executed amount in quote currency"""
    return self.get_total_executed_amount_quote()

@property
def trade_pnl_pct(self) -> Decimal:
    """Trade PnL percentage (without fees)"""
    # Compares average executed price vs current mid price

def get_net_pnl_quote(self) -> Decimal:
    """Net PnL after fees"""
    return self.trade_pnl_quote - self.cum_fees_quote

def get_average_executed_price(self) -> Decimal:
    """Weighted average execution price"""
    # Sum of (price * amount) / total amount
```

---

## Error Handling

### Validation on Init

```python
def __init__(self, ...):
    # Check minimum order size
    if self.config.order_amount_quote < trading_rules.min_order_size:
        self.close_execution_by(CloseType.FAILED)
        self.logger().error("Order amount too small")
```

### Retry Management

```python
def evaluate_max_retries(self):
    if self._current_retries > self._max_retries:
        self.close_execution_by(CloseType.FAILED)
```

### Balance Validation

```python
def validate_sufficient_balance(self):
    # Create order candidate
    # Adjust for available balance
    if adjusted_amount == 0:
        self.close_execution_by(CloseType.INSUFFICIENT_BALANCE)
```

---

## Summary

### Architecture Highlights

1. **Inheritance-Based Design**: Extends ExecutorBase for common functionality
2. **Event-Driven**: Uses event handlers for order state updates
3. **Time-Based Scheduling**: Order plan with timestamp-based execution
4. **Dynamic Amount Calculation**: Adjusts for fills and failures
5. **State Management**: Clear state transitions (RUNNING → SHUTTING_DOWN → TERMINATED)
6. **Error Recovery**: Retry mechanism for failed orders
7. **Mode Support**: Both TAKER (market) and MAKER (limit) modes

### Key Design Patterns

- **Strategy Pattern**: Different order types based on mode
- **Observer Pattern**: Event handlers for order events
- **Template Method**: ExecutorBase provides framework, TWAPExecutor implements specifics
- **State Pattern**: Status-based execution flow

---

**Document Version**: 1.0  
**Last Updated**: 2024  
**Codebase**: Hummingbot Strategy V2 Framework


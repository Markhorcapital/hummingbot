# Volume Bot Architecture Documentation

## Overview

The Volume Bot is a trading executor that generates trading volume through balanced buy/sell operations. It is built on the same architectural foundation as the TWAP strategy, reusing proven patterns and components.

---

## Architecture Reuse Analysis

### ✅ Components Directly Reused

1. **ExecutorBase Class**
   - Base class for all executors
   - Provides event forwarding, order placement, price access
   - Location: `hummingbot/strategy_v2/executors/executor_base.py`

2. **RunnableBase Framework**
   - Async task execution framework
   - Status management (RUNNING, SHUTTING_DOWN, TERMINATED)
   - Control loop pattern
   - Location: `hummingbot/strategy_v2/runnable_base.py`

3. **Event Handling System**
   - Event forwarders for order events
   - Same event types: BuyOrderCreatedEvent, SellOrderCreatedEvent, etc.
   - Same event processing pattern

4. **Configuration Structure**
   - Pydantic-based configuration
   - Same validation patterns
   - Same field validators

5. **Error Handling**
   - Same retry mechanisms
   - Same failure handling
   - Same close types (FAILED, INSUFFICIENT_BALANCE, etc.)

6. **Logging System**
   - Same logger class (HummingbotLogger)
   - Same logging format
   - Same log levels

7. **State Management**
   - Similar state tracking approach
   - TrackedOrder for order management
   - Same state persistence patterns

---

## Architecture Comparison: TWAP vs Volume Bot

### TWAP Executor Architecture

```
TWAPExecutor(ExecutorBase)
├── Configuration: TWAPExecutorConfig
├── Order Plan: Dict[timestamp, TrackedOrder]
├── Execution: Time-weighted single direction
├── State: Order plan with timestamps
└── Flow: Create orders at scheduled times
```

### Volume Bot Executor Architecture

```
VolumeBotExecutor(ExecutorBase)
├── Configuration: VolumeBotExecutorConfig
├── State: VolumeBotState (cycles, batches)
├── Execution: Balanced buy/sell batches
├── State: Cycle-based batch execution
└── Flow: Execute buy batch → wait → execute sell batch
```

---

## Key Architectural Patterns

### 1. Executor Pattern (Reused from TWAP)

**Lifecycle**:
```
INITIALIZATION
    ↓
RUNNING
    ├──→ control_task() loop
    ├──→ Event handlers active
    └──→ Order execution
         ↓
SHUTTING_DOWN
    ├──→ Wait for orders to close
    └──→ Cleanup
         ↓
TERMINATED
```

**Implementation**:
```python
class VolumeBotExecutor(ExecutorBase):
    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            self._evaluate_execute_buys()
            self._evaluate_execute_sells()
            self._evaluate_balance_verification()
            self._evaluate_max_retries()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self._evaluate_all_orders_closed()
```

### 2. Event Handling Pattern (Reused from TWAP)

**Event Flow**:
```
Exchange/Connector
    ↓ (emits events)
Event Forwarder (ExecutorBase)
    ↓ (routes to executor)
VolumeBotExecutor Event Handler
    ↓ (processes event)
Update Internal State
```

**Event Handlers** (Same as TWAP):
- `process_order_created_event()` - Order created
- `process_order_completed_event()` - Order filled
- `process_order_failed_event()` - Order failed

### 3. State Management Pattern

**TWAP State**:
```python
self._order_plan: Dict[float, Optional[TrackedOrder]]
```

**Volume Bot State**:
```python
class VolumeBotState:
    current_cycle: int
    pending_buys: int
    pending_sells: int
    current_batch_buys: List[TrackedOrder]
    current_batch_sells: List[TrackedOrder]
```

### 4. Configuration Pattern (Reused from TWAP)

**Same Structure**:
- Pydantic BaseModel
- Field validators
- Computed properties
- Type safety with Literal types

---

## Execution Flow

### Volume Bot Execution Cycle

```
START
  ↓
INITIALIZE STATE
  ↓
VALIDATE BALANCE (both buy and sell)
  ↓
┌─────────────────────────────────┐
│ MAIN CONTROL LOOP (control_task)│
└─────────────────────────────────┘
  ↓
CHECK: Time to start buy cycle?
  ↓ YES
START BUY CYCLE
  ↓
EXECUTE BUY BATCH
  ├─ Buy 1 → Wait interval_between_buys
  ├─ Buy 2 → Wait interval_between_buys
  └─ Buy N → Complete batch
  ↓
UPDATE STATE (pending_buys += N)
  ↓
WAIT interval_between_cycles
  ↓
START SELL CYCLE
  ↓
EXECUTE SELL BATCH
  ├─ Sell 1 → Wait interval_between_sells
  ├─ Sell 2 → Wait interval_between_sells
  └─ Sell N → Complete batch
  ↓
UPDATE STATE (pending_sells += N)
  ↓
VERIFY BALANCE
  ↓
RESET PENDING COUNTS
  ↓
WAIT interval_between_cycles
  ↓
REPEAT
```

---

## Buy/Sell Balance Logic

### Core Rule

**For every N buys, execute N sells** (configurable ratio)

### Balance Verification

```python
def _verify_balance(self):
    net_position = self._state.pending_buys - self._state.pending_sells
    
    if abs(net_position) > 1:  # Tolerance
        self.logger().warning("Balance warning")
    else:
        self.logger().info("Balance maintained")
```

### State Tracking

```python
class VolumeBotState:
    pending_buys: int      # Buys executed but not yet balanced
    pending_sells: int     # Sells executed (should match buys)
    total_buys_executed: int
    total_sells_executed: int
```

---

## Configuration Modes

### Mode 1: FIXED (Default)

```python
{
    "mode": "FIXED",
    "buy_batch_size": 3,
    "sell_batch_size": 3
}
```

**Behavior**: Fixed batch sizes for each cycle

### Mode 2: VARIABLE

```python
{
    "mode": "VARIABLE",
    "min_batch_size": 2,
    "max_batch_size": 5,
    "randomize": true
}
```

**Behavior**: Random batch sizes within min/max range

### Mode 3: TIME_BASED

```python
{
    "mode": "TIME_BASED",
    "buys_per_hour": 20,
    "maintain_balance": true
}
```

**Behavior**: Execute at specified rate per hour

### Mode 4: VOLUME_TARGET

```python
{
    "mode": "VOLUME_TARGET",
    "daily_volume_target": "50000",
    "maintain_balance": true
}
```

**Behavior**: Target daily volume, auto-adjust rate

---

## Code Structure

### File Organization (Same as TWAP)

```
hummingbot/strategy_v2/executors/volume_bot_executor/
├── __init__.py
├── data_types.py          # VolumeBotExecutorConfig
└── volume_bot_executor.py # VolumeBotExecutor
```

### Class Hierarchy (Same as TWAP)

```
RunnableBase
    ↓
ExecutorBase
    ↓
VolumeBotExecutor
```

---

## Integration Points

### 1. Strategy Integration

```python
class MyStrategy(StrategyV2Base):
    def determine_executor_actions(self):
        config = VolumeBotExecutorConfig(
            connector_name="binance",
            trading_pair="BTC-USDT",
            trade_amount_quote=Decimal("100"),
            buy_batch_size=3,
            sell_batch_size=3,
            # ... other config
        )
        return [CreateExecutorAction(executor_config=config)]
```

### 2. ExecutorBase Methods Used

- `place_order()` - Place orders via strategy
- `get_price()` - Get market price
- `get_trading_rules()` - Get exchange rules
- `adjust_order_candidates()` - Validate balance
- Event handlers (inherited)

---

## Error Handling (Same as TWAP)

### Retry Mechanism

```python
def process_order_failed_event(self, event):
    # Find failed order
    # Move to failed_orders list
    # Increment retry counter
    self._current_retries += 1

def _evaluate_max_retries(self):
    if self._current_retries > self._max_retries:
        self.close_execution_by(CloseType.FAILED)
```

### Balance Validation

```python
async def validate_sufficient_balance(self):
    # Check buy balance (quote currency)
    # Check sell balance (base currency)
    # Fail if insufficient
```

---

## Performance Metrics

### Volume Metrics

```python
@property
def total_volume_quote(self) -> Decimal:
    """Total volume generated"""
    return (total_buys + total_sells) * trade_amount_quote

@property
def net_position(self) -> int:
    """Net position (buys - sells)"""
    return total_buys - total_sells

@property
def balance_ratio(self) -> float:
    """Balance ratio (sells/buys)"""
    return total_sells / total_buys
```

---

## Testing Strategy

### Unit Tests (Same Pattern as TWAP)

```python
class TestVolumeBotExecutor:
    def test_initialization()
    def test_buy_batch_execution()
    def test_sell_batch_execution()
    def test_balance_verification()
    def test_event_handling()
    def test_error_recovery()
```

### Integration Tests

```python
class TestVolumeBotIntegration:
    def test_full_cycle_execution()
    def test_balance_maintenance()
    def test_multiple_cycles()
```

---

## Summary

### Architecture Reuse ✅

- ✅ Same executor base class
- ✅ Same event handling system
- ✅ Same configuration structure
- ✅ Same error handling patterns
- ✅ Same logging system
- ✅ Same state management approach

### Key Differences

- **TWAP**: Time-weighted, single direction
- **Volume Bot**: Batch-based, dual direction (balanced)

### Consistency Maintained

- Same coding patterns
- Same naming conventions
- Same directory structure
- Same testing approach
- Same documentation style

---

**Document Version**: 1.0  
**Based On**: TWAP Strategy Architecture  
**Last Updated**: 2024


# Arbitrage Executor — `buy_only` Mode Updates

This document describes the changes added to support **buy-only relative value** trading: the executor still compares prices across two connectors, but when `buy_only=True` it only places a buy on the cheaper venue and uses the other connector as a price reference (no sell leg).

---

## Files Changed

| File | Summary |
|------|---------|
| `hummingbot/strategy_v2/executors/arbitrage_executor/data_types.py` | Added `buy_only` config field |
| `hummingbot/strategy_v2/executors/arbitrage_executor/arbitrage_executor.py` | Buy-only execution, balance, fees, completion, and status logic |
| `controllers/generic/arbitrage_controller.py` | Controller config and executor creation pass-through |
| `test/hummingbot/strategy_v2/executors/arbitrage_executor/test_arbitrage_executor.py` | Unit tests for buy-only behavior |

---

## 1. `ArbitrageExecutorConfig` (`data_types.py`)

**Added:**

```python
buy_only: bool = False
```

- Default `False` preserves existing two-legged arbitrage behavior.
- Set `True` to enable buy-only mode.

---

## 2. `ArbitrageExecutor` (`arbitrage_executor.py`)

### `validate_sufficient_balance()`

| `buy_only=False` | `buy_only=True` |
|------------------|-----------------|
| Requires base asset on selling exchange **and** quote asset on buying exchange | Requires quote asset on buying exchange only |

### `update_tx_cost()`

| `buy_only=False` | `buy_only=True` |
|------------------|-----------------|
| Includes buy + sell fees in `_last_tx_cost` | Includes buy fees only; `_last_sell_fee` set to `0` |

### `execute_arbitrage()`

| `buy_only=False` | `buy_only=True` |
|------------------|-----------------|
| Places LIMIT buy + MARKET sell | Places LIMIT buy only |

### `check_order_status()`

| `buy_only=False` | `buy_only=True` |
|------------------|-----------------|
| Completes when **both** buy and sell are filled | Completes when **buy** is filled |

### `get_net_pnl_quote()`

| `buy_only=False` | `buy_only=True` |
|------------------|-----------------|
| Returns realized arb PnL (sell − buy − fees) | Returns `0` (no locked profit without a sell leg) |

### `process_order_failed_event()`

| `buy_only=False` | `buy_only=True` |
|------------------|-----------------|
| Retries failed buy or sell orders | Retries failed buy orders only |

### `get_custom_info()`

- Added `"buy_only": self.config.buy_only` to executor status metadata.

### `to_format_status()`

- Shows mode label: **"Buy Only"** or **"Full Arbitrage"**.
- Buy-only status shows a **Reference** connector instead of a sell arrow.
- Total profit line is hidden on completion when `buy_only=True` (no realized arb PnL).

### Unchanged (both modes)

- Spread detection: buy price on `buying_market`, sell price on `selling_market` (reference).
- Quote asset normalization via `RateOracle`.
- Profitability trigger: `_current_profitability > min_profitability`.

---

## 3. `ArbitrageController` (`arbitrage_controller.py`)

### `ArbitrageControllerConfig`

**Added:**

```python
buy_only: bool = False
```

### `create_arbitrage_executor_action()`

- Passes `buy_only=self.config.buy_only` into each `ArbitrageExecutorConfig`.

### `determine_executor_actions()`

- No change to executor creation logic: both monitoring executors are still created (buy on `exchange_pair_1` vs reference `exchange_pair_2`, and the reverse).
- When `buy_only=True`, each executor only buys on its configured venue; the paired connector remains a price reference.

---

## 4. Tests (`test_arbitrage_executor.py`)

**Added tests:**

| Test | What it verifies |
|------|------------------|
| `test_control_task_profitable_buy_only` | Profitable spread places buy only (no sell `order_id`) |
| `test_control_task_complete_buy_only` | Executor completes when buy fills |
| `test_validate_sufficient_balance_buy_only_skips_sell_balance` | Selling-exchange balance is not checked |
| `test_update_tx_cost_buy_only` | Transaction cost uses buy fee only |

**Updated:**

- `setUp()` sets `buy_only = False` on the default mock config.

---

## Usage

### Controller YAML / config

```yaml
buy_only: true
min_profitability: 0.01
exchange_pair_1:
  connector_name: binance
  trading_pair: PENGU-USDT
exchange_pair_2:
  connector_name: jupiter_solana_mainnet-beta
  trading_pair: PENGU-USDC
```

### Direct executor config

```python
ArbitrageExecutorConfig(
    timestamp=...,
    buying_market=ConnectorPair(connector_name="binance", trading_pair="PENGU-USDT"),
    selling_market=ConnectorPair(connector_name="jupiter_solana_mainnet-beta", trading_pair="PENGU-USDC"),
    order_amount=Decimal("100"),
    min_profitability=Decimal("0.01"),
    buy_only=True,
)
```

---

## Behavior Summary

| Aspect | `buy_only=False` | `buy_only=True` |
|--------|------------------|-----------------|
| Purpose | Lock arb spread (buy low + sell high) | Accumulate on cheap venue vs reference |
| Orders | Buy + sell | Buy only |
| Balance | Base (sell) + quote (buy) | Quote (buy) only |
| Fees in threshold | Buy + sell | Buy only |
| Completion | Both legs filled | Buy filled |
| PnL | Realized | Unrealized (inventory on buy venue) |

---

## Running Tests

From the repo root, with the `hummingbot` conda environment active and Cython extensions compiled:

```bash
conda activate hummingbot
./compile

python -m pytest test/hummingbot/strategy_v2/executors/arbitrage_executor/test_arbitrage_executor.py -v -k "buy_only"
```

---

## Notes

- `selling_market` is still required in buy-only mode; it is used for spread calculation, not execution.
- Buy-only mode may trigger more often than full arb for the same `min_profitability`, because sell-side fees are excluded from the cost estimate.
- Inventory accumulates on the buying connector; plan exits separately (manual sell, grid, take-profit executor, etc.).

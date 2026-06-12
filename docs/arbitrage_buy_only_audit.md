# Arbitrage `buy_only` Mode — Audit Findings

Audit of the `buy_only` implementation described in [`arbitrage_buy_only_updates.md`](./arbitrage_buy_only_updates.md), verified against:

- `hummingbot/strategy_v2/executors/arbitrage_executor/data_types.py`
- `hummingbot/strategy_v2/executors/arbitrage_executor/arbitrage_executor.py`
- `controllers/generic/arbitrage_controller.py`
- `test/hummingbot/strategy_v2/executors/arbitrage_executor/test_arbitrage_executor.py`

---

## Verdict

**Executor-level implementation is largely correct and matches the feature doc.** The `buy_only` flag is threaded cleanly, defaults preserve backward compatibility, and unit tests cover the main execution paths.

**The feature is not production-safe as a controller-driven strategy** without operators understanding dual-executor behavior, inventory risk, and gaps in status reporting, testing, and edge-case handling.

| Use case | Recommendation |
|----------|----------------|
| Single manually configured executor | Reasonable for controlled testing |
| Controller with `buy_only: true` | Treat as **beta** until P0/P1 items below are addressed |

---

## Doc vs Code Accuracy

| Doc claim | Verified | Notes |
|-----------|----------|-------|
| `buy_only` on `ArbitrageExecutorConfig`, default `False` | Yes | Backward compatible |
| Balance check skips sell venue | Yes | Quote on buy venue only |
| Tx cost = buy fee only | Yes | `_last_sell_fee` set to `0` |
| Execute = LIMIT buy only | Yes | No sell leg placed |
| Complete on buy fill only | Yes | `check_order_status()` |
| PnL returns `0` | Yes | No realized arb without sell |
| Sell retry skipped on failure | Yes | `process_order_failed_event()` |
| Controller passes `buy_only` through | Yes | `create_arbitrage_executor_action()` |
| Controller still spawns both direction executors | Yes — intentional | See Critical #1 |
| Spread detection unchanged | Yes | Reference sell quote still fetched |
| Quote normalization via `RateOracle` | Yes — decision path only | Status display differs; see High #5 |

The feature doc faithfully describes the implementation. No material misrepresentation was found.

---

## Critical

### 1. Dual executors can buy on both venues

With `buy_only=True`, the controller still spawns two executors:

- Buy on `exchange_pair_1` vs reference `exchange_pair_2`
- Buy on `exchange_pair_2` vs reference `exchange_pair_1`

Each executor only places a **buy** when its direction looks profitable. This is a “buy whichever side is cheap” design, not “buy only on one exchange.”

**Risk:** Capital deployed on both venues; `total_amount_quote` applies per executor (~2× exposure). Stale or asymmetric quotes can trigger both sides in the same window.

**Remediation (Architect):** Document explicitly, or add `single_venue_only` / spawn only one executor direction. Cap total inventory across venues.

---

### 2. Inventory accumulates with no automated exit

Each completed buy-only executor closes as `CloseType.COMPLETED` with `get_net_pnl_quote() == 0`. There is no `POSITION_HOLD`, notifier, or link to a grid/take-profit executor.

**Risk:** Silent inventory buildup; dashboards show zero PnL while exposure grows.

**Remediation (Developer):** Emit inventory events; integrate with `positions_held` or a companion exit controller; report mark-to-reference unrealized PnL.

---

### 3. LIMIT buy can leave executor stuck in `SHUTTING_DOWN`

`execute_arbitrage()` places a **LIMIT** buy at `_last_buy_price`, then sets `SHUTTING_DOWN`. `check_order_status()` only completes when `is_filled`. There is no cancel, reprice, or timeout if the limit never fills.

**Risk:** Executor hangs indefinitely; controller treats the direction as active (`_len_active_buy_arbitrages > 0`) and stops spawning replacements.

**Remediation (Developer):** Timeout + cancel; or MARKET/IOC for buy-only; or return to `RUNNING` if unfilled after N ticks.

---

## High

### 4. Imbalance / throttle semantics are wrong for buy-only

`update_arbitrage_stats()` labels directions `buy_arbitrages` / `sell_arbitrages` by `buying_market`, but in buy-only mode **both are buys** on different venues:

- `buy_arbitrages` → buys on `exchange_pair_1`
- `sell_arbitrages` → buys on `exchange_pair_2`

`max_executors_imbalance` and `delay_between_executors` gate on this counter.

**Risk:** Operators read “imbalance” as buy vs sell leg imbalance; throttling may starve one venue while the other keeps buying.

**Remediation (Developer):** Rename counters for buy-only mode or disable imbalance gating when `buy_only=True`. Document in the feature doc.

---

### 5. Status display does not match trading decision math

Decision path normalizes quote assets in `update_trade_pnl_pct()`. `to_format_status()` uses raw prices:

```text
trade_pnl_pct = (sell_price - buy_price) / buy_price
```

For pairs like `PENGU-USDT` vs `PENGU-USDC`, the UI can show a **wrong** spread while the executor fires (or not) on normalized math.

**Risk:** Operator trusts status panel and misjudges edge.

**Remediation (Developer):** Use `_trade_pnl_pct` and `_current_profitability` in status output (already exposed in `custom_info`).

---

### 6. USDT/USDC pairs rely on loose token matching

The doc example uses `PENGU-USDT` / `PENGU-USDC`. Validation passes via `"USD" in token` in `_are_tokens_interchangeable()`. Spread math depends on `RateOracle` for USDC–USDT conversion.

**Risk:** Depeg, oracle lag, or missing rate → false signals or exceptions in `get_quote_asset_conversion_rate()`.

**Remediation (Developer):** Document oracle dependency; tighten stablecoin rules; fail fast if conversion rate is unavailable.

---

### 7. `gas_conversion_price` unguarded on AMM buy venue

For AMM connectors (e.g. Jupiter), tx cost is computed as:

```text
gas_cost.amount / self.config.gas_conversion_price
```

No guard when `gas_conversion_price` is `None` or zero.

**Risk:** Crash or wrong profitability threshold on AMM buy-only paths.

**Remediation (Developer):** Validate in controller before creating executor; skip or fail fast if rate is missing.

---

## Medium

### 8. `get_net_pnl_quote() == 0` hides completed buy cost

Correct for realized PnL without a sell leg, but controller `to_format_status()` aggregates `custom_info` with no inventory or cost-basis column.

**Remediation (Developer):** Add `buy_cost_quote`, `reference_price`, or `unrealized_edge_pct` to `get_custom_info()` for buy-only completions.

---

### 9. Reference connector required at runtime

Buy-only still calls `get_resulting_price_for_amount(..., is_buy=False)` on `selling_market` every tick. Reference outage → `"Error calculating profitability"` and no trades.

**Remediation:** Document hard dependency; consider pause/degraded mode when reference is unavailable.

---

### 10. Lower fee bar → more frequent triggers

Sell-side fees are excluded from `_current_profitability`, so the same `min_profitability` fires more easily than full arb (noted in feature doc line 184). No separate threshold for buy-only.

**Remediation (Architect):** Optional `min_profitability_buy_only` or auto-adjust when `buy_only=True`.

---

### 11. Controller can append `None` actions on error

If `create_arbitrage_executor_action()` raises, it logs and returns implicitly `None`, which is still appended to `executor_actions`. Pre-existing; affects buy-only controller usage.

**Remediation (Developer):** Return `None` explicitly and filter before append, or re-raise after logging.

---

### 12. `to_format_status()` error path returns `None`

On the error branch, `return lines.extend(msg)` returns `None` instead of a list. Pre-existing; can break status UI.

**Remediation (Developer):** `lines.extend(msg); return lines`.

---

## Low

### 13. Test coverage gaps

**Present:**

| Test | Coverage |
|------|----------|
| `test_control_task_profitable_buy_only` | No sell order placed |
| `test_control_task_complete_buy_only` | Completes on buy fill |
| `test_validate_sufficient_balance_buy_only_skips_sell_balance` | Skips sell balance |
| `test_update_tx_cost_buy_only` | Buy fee only |

**Missing:**

- `get_net_pnl_quote()` returns `0` when `buy_only=True` and completed
- `process_order_failed_event` does not retry/increment on sell failure in buy-only
- `to_format_status` “Buy Only” / “Reference” labels
- Quote-normalized profitability vs status display
- **`ArbitrageController` tests** with `buy_only=True` (dual spawn, imbalance, delay)
- LIMIT non-fill / stuck `SHUTTING_DOWN`
- Partial buy fill behavior

---

### 14. Documentation gaps in feature doc

The feature doc is accurate on executor behavior but should also cover:

- Dual executor = possible buys on **both** venues (~2× `total_amount_quote` exposure)
- `max_executors_imbalance` counts pair_1 vs pair_2 **buys**, not buy vs sell
- Reference connector must be live for spread checks
- LIMIT order hang risk
- USDT/USDC oracle dependency for the example YAML

---

## What Was Done Well

1. **Backward compatible** — `buy_only: bool = False` everywhere; full arb unchanged.
2. **Consistent flag threading** — config → controller → executor → `custom_info`.
3. **Surgical executor changes** — balance, fees, execution, completion, PnL, failed-event, and status paths are consistently gated on `buy_only`.
4. **Honest feature doc** on inventory and exit planning (Notes section).
5. **Unit tests** cover the four highest-risk executor branches for the new flag.

---

## Pre-Merge Checklist

| Priority | Item | Owner |
|----------|------|-------|
| P0 | Document or constrain dual-venue buy exposure in controller | Architect |
| P0 | LIMIT buy timeout / cancel / reprice in buy-only shutdown path | Developer |
| P1 | Fix status to use normalized `_trade_pnl_pct` / `_current_profitability` | Developer |
| P1 | Clarify or fix imbalance gating for buy-only | Developer |
| P1 | Add `ArbitrageController` tests for `buy_only=True` | Developer |
| P2 | Unrealized PnL / inventory in `custom_info` | Developer |
| P2 | Guard `gas_conversion_price` for AMM buys | Developer |
| P2 | Tests for PnL=0, failed-event sell skip, status labels | Developer |

---

## Summary

Executor-level `buy_only` **does what the feature doc says**. The main audit concerns are **operational design**: dual monitoring executors imply dual accumulation, imbalance controls use misleading semantics, LIMIT orders can stall, and the UI can show PnL that disagrees with the trigger logic—especially for cross-quote pairs like USDT/USDC.

**Developer:** Status math, stuck-limit handling, tests, and `None` action filtering.
**Architect:** Confirm dual-executor buy-only is the intended product contract; if yes, document capital limits and imbalance behavior in the feature doc.

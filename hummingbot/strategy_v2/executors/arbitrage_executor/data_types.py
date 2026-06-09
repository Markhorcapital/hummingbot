from decimal import Decimal
from typing import Literal, Optional

from pydantic import Field, model_validator

from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class ArbitrageExecutorConfig(ExecutorConfigBase):
    type: Literal["arbitrage_executor"] = "arbitrage_executor"
    buying_market: ConnectorPair
    selling_market: ConnectorPair
    order_amount: Decimal
    min_profitability: Decimal
    gas_conversion_price: Optional[Decimal] = None

    # --- Auto-sizing (Option 1 + Option 3) -----------------------------------
    # When auto_size is False, the executor behaves exactly as before: it
    # always quotes/executes at the fixed order_amount above.
    #
    # When auto_size is True, every control tick:
    #   1. samples live cex_mid and dex_spot via a tiny probe quote
    #   2. (optional) applies an analytic peak-profit gate to skip walks
    #   3. walks the CEX order book and queries DEX VWAP at each cumulative
    #      size to find the size s* that maximises net profit
    #   4. applies sizing / profit / slippage caps
    #   5. re-probes dex_spot for drift sanity
    #   6. fires both legs market, with the DEX leg using a slippage-adjusted
    #      limit price so Gateway will revert rather than execute at a worse
    #      fill than expected.
    auto_size: bool = False

    # Hard size band. When auto_size is True the walk is clamped to this band.
    min_order_amount: Optional[Decimal] = None
    max_order_amount: Optional[Decimal] = None

    # Cheap gate: |live_basis| must clear this to even attempt a walk.
    min_basis_bps: Decimal = Decimal("0")

    # Absolute floor in quote currency. min_profitability above is the bps floor.
    min_net_profit_quote: Decimal = Decimal("0")

    # Max fraction of available balance the executor may consume per arb.
    # 1.0 = use all of it, 0.5 = leave half in reserve. Applied per side.
    max_balance_pct: Decimal = Decimal("0.95")

    # DEX leg limit-price slippage cap. Set the swap's limit price so a worse
    # fill (e.g. front-run, re-org) reverts on-chain rather than executing.
    max_slippage_pct: Decimal = Decimal("0.01")

    # Re-probe dex_spot just before firing; abort if it has drifted more than
    # this many bps from the value used during the walk.
    max_drift_bps: Decimal = Decimal("30")

    # Amount used as the spot probe (and the smallest walk step). When unset
    # it defaults to min_order_amount.
    tiny_probe_amount: Optional[Decimal] = None

    # Number of CEX book levels the walker may consume before bailing out.
    # Stops the walk going arbitrarily deep on a thin book. Default tuned for
    # ops cost: a full walk costs N Gateway RPCs (each `cum_size` is a unique
    # cache key, so they are NOT shared across levels). At N=8 with a probe
    # and a 2-stage drift check, a single executing tick is ~11 RPCs.
    max_walk_levels: int = Field(default=8, ge=1)

    # Cached slippage coefficient k freshness. Estimated from each successful
    # walk and reused by the analytic gate until this many seconds elapse.
    k_cache_ttl: float = 30.0

    @model_validator(mode="after")
    def _validate_auto_size_band(self):
        if self.auto_size:
            min_amt = self.min_order_amount if self.min_order_amount is not None else self.order_amount
            max_amt = self.max_order_amount if self.max_order_amount is not None else self.order_amount
            if min_amt <= 0:
                raise ValueError("min_order_amount must be > 0 when auto_size is enabled")
            if max_amt < min_amt:
                raise ValueError("max_order_amount must be >= min_order_amount when auto_size is enabled")
        return self

import os
from decimal import Decimal
from typing import Literal, Optional

from pydantic import Field, field_validator, model_validator

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase


class ConvergenceExecutorConfig(ExecutorConfigBase):
    """One-sided CEX taker sweep toward a DEX TWAP reference (dex_fair)."""

    type: Literal["convergence_executor"] = "convergence_executor"
    connector_name: str
    trading_pair: str

    # DexPriceFeed (same semantics as pmm_dynamic)
    dex_rpc_url: Optional[str] = None
    dex_pool_address: str
    dex_base_token_address: str
    dex_quote_token_address: str
    dex_eth_usdt_trading_pair: str = "ETH-USDT"
    dex_poll_interval_seconds: int = 2
    dex_twap_seconds: int = 180
    dex_price_max_stale_seconds: int = 30
    dex_sanity_max_divergence_pct: Decimal = Decimal("0.15")

    # Sweep behaviour
    min_gap_bps: Decimal = Decimal("30")
    # Only place when |cex−dex| gap (bps) is strictly above the reference (spawn or last fill).
    require_gap_widening: bool = True
    # Absolute gap (bps) at controller spawn; set by controller when creating the executor.
    spawn_gap_bps: Optional[Decimal] = None
    # When True (default), size from zone liquidity + caps only — no profit/edge gate.
    rebalance_only: bool = True
    min_edge_bps_after_fees: Decimal = Decimal("10")
    max_amount_quote: Decimal = Decimal("50")
    zone_quote_threshold_usdt: Decimal = Decimal("100")
    max_balance_pct: Decimal = Decimal("0.5")
    max_walk_levels: int = Field(default=8, ge=1)
    sweep_chunks: int = Field(default=3, ge=1)
    allowed_sides: Literal["both", "buy", "sell"] = "both"
    # Log every control-tick evaluation at DEBUG when True.
    debug_verbose: bool = False
    # Set by the executor when the sweep direction is chosen (orchestrator position hold).
    side: Optional[TradeType] = None

    @model_validator(mode="after")
    def resolve_dex_rpc_url(self):
        if self.dex_rpc_url is None or str(self.dex_rpc_url).strip() == "":
            url = os.getenv("DEX_RPC_URL") or os.getenv("WEB3_PROVIDER")
            if not url:
                raise ValueError("dex_rpc_url is required (or set DEX_RPC_URL / WEB3_PROVIDER)")
            object.__setattr__(self, "dex_rpc_url", str(url).strip())
        return self

    @field_validator("dex_sanity_max_divergence_pct", mode="before")
    @classmethod
    def parse_dex_sanity(cls, v):
        if isinstance(v, str):
            if v == "":
                return Decimal("0.15")
            return Decimal(v)
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        return v

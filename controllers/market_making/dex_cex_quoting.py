"""Regime A/B order price formulas (pure logic, no Hummingbot imports)."""
from __future__ import annotations

from decimal import Decimal
from typing import Literal

Regime = Literal["A", "B"]


def compute_regime_order_price(
    regime: Regime,
    is_buy: bool,
    spread: Decimal,
    cex_mid: Decimal,
    dex_fair: Decimal,
) -> Decimal:
    """
    Regime A (cex_mid > dex_fair):
      buy  = dex_fair × (1 - spread)
      sell = cex_mid  × (1 + spread)
    Regime B (dex_fair > cex_mid):
      buy  = cex_mid  × (1 - spread)
      sell = dex_fair × (1 + spread)
    """
    if regime == "A":
        anchor = dex_fair if is_buy else cex_mid
    else:
        anchor = cex_mid if is_buy else dex_fair
    side_mult = Decimal("-1") if is_buy else Decimal("1")
    return anchor * (1 + side_mult * spread)

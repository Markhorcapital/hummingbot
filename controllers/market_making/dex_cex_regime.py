"""Regime A/B hysteresis (pure logic, no Hummingbot imports)."""
from __future__ import annotations

from decimal import Decimal
from typing import Literal, Optional, Tuple

Regime = Literal["A", "B"]


def update_regime_with_hysteresis(
    basis_pct: Optional[Decimal],
    current_regime: Optional[Regime],
    confirm_count: int,
    hysteresis_bps: int,
    hysteresis_ticks: int,
) -> Tuple[Optional[Regime], int]:
    """
    Regime A: cex_mid > dex_fair (basis_pct > 0).
    Regime B: dex_fair > cex_mid (basis_pct < 0).

    Switch only if |basis_pct| > threshold for hysteresis_ticks consecutive polls.
    """
    if basis_pct is None:
        return current_regime, confirm_count

    threshold = Decimal(hysteresis_bps) / Decimal(10000)
    if basis_pct > threshold:
        candidate: Regime = "A"
    elif basis_pct < -threshold:
        candidate = "B"
    else:
        # Dead band: keep regime when set; bootstrap from basis sign on first tick
        if current_regime is None and basis_pct != 0:
            return ("A" if basis_pct > 0 else "B"), 0
        return current_regime, 0

    if current_regime is None:
        return candidate, 0

    if candidate == current_regime:
        return current_regime, 0

    confirm_count += 1
    if confirm_count >= hysteresis_ticks:
        return candidate, 0
    return current_regime, confirm_count

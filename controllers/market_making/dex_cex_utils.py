"""Shared helpers for DEX/CEX PMM (no Hummingbot connector imports)."""
from __future__ import annotations

import time
from typing import Dict, Optional


class WarningThrottler:
    """Rate-limit repeated warning logs (audit P2-06 / P2-07)."""

    def __init__(self, interval_seconds: float = 60.0):
        self._interval = interval_seconds
        self._last: Dict[str, float] = {}

    def should_log(self, key: str, now: Optional[float] = None) -> bool:
        now = now if now is not None else time.time()
        last = self._last.get(key, 0.0)
        if now - last >= self._interval:
            self._last[key] = now
            return True
        return False

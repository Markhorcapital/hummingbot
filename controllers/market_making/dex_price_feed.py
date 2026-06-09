"""
DEX fair price feed: Uniswap V3 pool (TWAP) × CEX ETH/USDT mid.

Used by pmm_dynamic DEX/CEX strategy (Phase 1+).
"""
from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Deque, Optional, Tuple

from controllers.market_making.uniswap_v3_pool_reader import PoolSpotResult, PoolTwapResult, UniswapV3PoolReader


class TwapSource(str, Enum):
    OBSERVE = "observe"
    FALLBACK = "fallback"
    NONE = "none"


def compute_basis_pct(
    cex_mid: Optional[Decimal],
    dex_fair: Optional[Decimal],
) -> Optional[Decimal]:
    """(cex_mid - dex_fair) / dex_fair — recompute every tick, not only on poll."""
    if cex_mid is None or dex_fair is None or dex_fair <= 0:
        return None
    return (cex_mid - dex_fair) / dex_fair


@dataclass
class DexPriceFeedConfig:
    dex_rpc_url: str
    dex_pool_address: str
    base_token_address: str
    quote_token_address: str
    connector_name: str
    dex_eth_usdt_trading_pair: str = "ETH-USDT"
    dex_twap_seconds: int = 180
    dex_poll_interval_seconds: int = 2
    dex_price_max_stale_seconds: int = 30
    dex_sanity_max_divergence_pct: Decimal = Decimal("0.15")
    verbose_logging: bool = False


@dataclass
class DexPriceSnapshot:
    dex_fair: Optional[Decimal]
    base_in_quote: Optional[Decimal]
    eth_usdt_mid: Optional[Decimal]
    spot_ali_usdt: Optional[Decimal]
    twap_source: TwapSource
    avg_tick: Optional[float] = None
    spot_tick: Optional[int] = None
    basis_pct: Optional[Decimal] = None
    last_poll_ts: float = 0.0
    last_success_ts: float = 0.0
    error: Optional[str] = None


@dataclass
class _SpotSample:
    timestamp: float
    spot_ali_usdt: Decimal


class DexPriceFeed:
    def __init__(
        self,
        config: DexPriceFeedConfig,
        market_data_provider: Any,
        pool_reader: Optional[UniswapV3PoolReader] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.config = config
        self._mdp = market_data_provider
        self._logger = logger or logging.getLogger(__name__)
        self._pool_reader = pool_reader or UniswapV3PoolReader(
            rpc_url=config.dex_rpc_url,
            pool_address=config.dex_pool_address,
            base_token_address=config.base_token_address,
            quote_token_address=config.quote_token_address,
            logger=self._logger,
        )
        self._samples: Deque[_SpotSample] = deque()
        self._snapshot = DexPriceSnapshot(
            dex_fair=None,
            base_in_quote=None,
            eth_usdt_mid=None,
            spot_ali_usdt=None,
            twap_source=TwapSource.NONE,
        )
        self._last_poll_monotonic: float = 0.0

    @property
    def pool_reader(self) -> UniswapV3PoolReader:
        return self._pool_reader

    @property
    def snapshot(self) -> DexPriceSnapshot:
        return self._snapshot

    def get_eth_usdt_mid(self) -> Decimal:
        from hummingbot.core.data_type.common import PriceType

        return self._mdp.get_price_by_type(
            self.config.connector_name,
            self.config.dex_eth_usdt_trading_pair,
            PriceType.MidPrice,
        )

    def _prune_samples(self, now: float) -> None:
        cutoff = now - self.config.dex_twap_seconds
        while self._samples and self._samples[0].timestamp < cutoff:
            self._samples.popleft()

    def _fallback_twap_ali_usdt(self, now: float, eth_usdt_mid: Decimal) -> Tuple[Decimal, Decimal]:
        """Return (approx base_in_quote, twap ali/usdt) from sample buffer."""
        self._prune_samples(now)
        if not self._samples:
            raise ValueError("No spot samples for fallback TWAP")
        total = sum((s.spot_ali_usdt for s in self._samples), Decimal(0))
        twap_ali_usdt = total / Decimal(len(self._samples))
        base_in_quote = twap_ali_usdt / eth_usdt_mid
        return base_in_quote, twap_ali_usdt

    def _record_spot_sample(self, spot_ali_usdt: Decimal, now: float) -> None:
        self._samples.append(_SpotSample(timestamp=now, spot_ali_usdt=spot_ali_usdt))
        self._prune_samples(now)

    def poll(self, cex_mid: Optional[Decimal] = None) -> DexPriceSnapshot:
        """
        Refresh dex_fair. Call on each controller tick (respect dex_poll_interval_seconds externally).
        """
        now = time.time()
        self._last_poll_monotonic = now
        eth_usdt_mid: Optional[Decimal] = None
        base_in_quote: Optional[Decimal] = None
        dex_fair: Optional[Decimal] = None
        spot_ali_usdt: Optional[Decimal] = None
        twap_source = TwapSource.NONE
        avg_tick: Optional[float] = None
        spot_tick: Optional[int] = None
        error: Optional[str] = None

        try:
            eth_usdt_mid = self.get_eth_usdt_mid()
        except Exception as e:
            error = f"ETH/USDT mid failed: {e}"
            self._logger.warning(error)

        spot_result: Optional[PoolSpotResult] = None
        if eth_usdt_mid is not None:
            try:
                spot_result = self._pool_reader.get_spot_base_in_quote()
                spot_tick = spot_result.tick
                spot_ali_usdt = spot_result.base_in_quote * eth_usdt_mid
                self._record_spot_sample(spot_ali_usdt, now)
            except Exception as e:
                self._logger.warning("slot0 spot read failed: %s", e)

        twap_result: Optional[PoolTwapResult] = None
        if eth_usdt_mid is not None:
            try:
                twap_result = self._pool_reader.get_twap_base_in_quote(self.config.dex_twap_seconds)
                base_in_quote = twap_result.base_in_quote
                dex_fair = base_in_quote * eth_usdt_mid
                twap_source = TwapSource.OBSERVE
                avg_tick = twap_result.avg_tick
            except Exception as e:
                self._logger.warning(
                    "observe() TWAP failed (%ss), trying fallback: %s",
                    self.config.dex_twap_seconds,
                    e,
                )
                try:
                    if len(self._samples) > 0:
                        base_in_quote, dex_fair = self._fallback_twap_ali_usdt(now, eth_usdt_mid)
                        twap_source = TwapSource.FALLBACK
                    else:
                        error = (error or "") + f"; observe failed: {e}; no fallback samples"
                except Exception as fb_e:
                    error = (error or "") + f"; observe: {e}; fallback: {fb_e}"

        basis_pct = compute_basis_pct(cex_mid, dex_fair)

        last_success_ts = self._snapshot.last_success_ts
        if dex_fair is not None:
            last_success_ts = now

        self._snapshot = DexPriceSnapshot(
            dex_fair=dex_fair,
            base_in_quote=base_in_quote,
            eth_usdt_mid=eth_usdt_mid,
            spot_ali_usdt=spot_ali_usdt,
            twap_source=twap_source,
            avg_tick=avg_tick,
            spot_tick=spot_tick,
            basis_pct=basis_pct,
            last_poll_ts=now,
            last_success_ts=last_success_ts,
            error=error,
        )

        if dex_fair is not None:
            if self.config.verbose_logging:
                self._logger.info(
                    "[DEX/CEX POLL] ok dex_fair=%s twap_source=%s eth_usdt=%s basis_pct=%s "
                    "base_in_quote=%s spot_ali_usdt=%s",
                    dex_fair,
                    twap_source.value,
                    eth_usdt_mid,
                    basis_pct,
                    base_in_quote,
                    spot_ali_usdt,
                )
            else:
                self._logger.debug(
                    "DexPriceFeed dex_fair=%s twap_source=%s base_in_quote=%s eth_usdt=%s basis_pct=%s",
                    dex_fair,
                    twap_source.value,
                    base_in_quote,
                    eth_usdt_mid,
                    basis_pct,
                )
        elif self.config.verbose_logging:
            self._logger.warning(
                "[DEX/CEX POLL] failed eth_usdt=%s twap_source=%s error=%s spot_samples=%s",
                eth_usdt_mid,
                twap_source.value,
                error,
                len(self._samples),
            )
        return self._snapshot

    def get_dex_fair(self) -> Optional[Decimal]:
        return self._snapshot.dex_fair

    def is_stale(self, now: Optional[float] = None) -> bool:
        now = now or time.time()
        if self._snapshot.last_success_ts <= 0:
            return True
        return (now - self._snapshot.last_success_ts) > self.config.dex_price_max_stale_seconds

    def sanity_ok(self, cex_mid: Decimal) -> bool:
        dex_fair = self._snapshot.dex_fair
        if dex_fair is None or dex_fair <= 0:
            return False
        divergence = abs(cex_mid - dex_fair) / dex_fair
        return divergence <= self.config.dex_sanity_max_divergence_pct

    def should_poll(self, now: Optional[float] = None) -> bool:
        now = now or time.time()
        if self._snapshot.last_poll_ts <= 0:
            return True
        return (now - self._snapshot.last_poll_ts) >= self.config.dex_poll_interval_seconds

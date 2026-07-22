import logging
import time
from decimal import Decimal
from typing import Dict, Optional, Tuple

import aiohttp

from hummingbot.logger import HummingbotLogger

# Hummingbot connector name -> CoinGecko exchange id
CONNECTOR_TO_COINGECKO_EXCHANGE: Dict[str, str] = {
    "mexc": "mxc",
    "gate_io": "gate",
    "htx": "huobi",
    "huobi": "huobi",
    "binance": "binance",
    "kucoin": "kucoin",
    "okx": "okx",
    "bybit": "bybit",
    "bitget": "bitget",
    "ascend_ex": "ascendex",
}


class CoinGeckoVolumeClient:
    """Fetches 24h pair volume (USD) from CoinGecko tickers API, with short TTL cache."""

    _logger: Optional[HummingbotLogger] = None
    _cache: Dict[Tuple[str, str, str], Tuple[Decimal, float]] = {}
    _cache_ttl_seconds: float = 60.0
    _base_url: str = "https://api.coingecko.com/api/v3"

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @classmethod
    def exchange_id_for_connector(cls, connector_name: str) -> Optional[str]:
        return CONNECTOR_TO_COINGECKO_EXCHANGE.get(connector_name)

    @classmethod
    async def get_pair_volume_usd(
        cls,
        connector_name: str,
        trading_pair: str,
        coin_id: str,
    ) -> Optional[Decimal]:
        """
        Return 24h converted USD volume for trading_pair on the given exchange.
        Returns None if unavailable / API error.
        """
        exchange_id = cls.exchange_id_for_connector(connector_name)
        if exchange_id is None:
            cls.logger().warning(f"No CoinGecko exchange mapping for connector '{connector_name}'.")
            return None

        cache_key = (coin_id.lower(), exchange_id, trading_pair.upper())
        cached = cls._cache.get(cache_key)
        now = time.time()
        if cached is not None and now - cached[1] < cls._cache_ttl_seconds:
            return cached[0]

        base, quote = trading_pair.upper().split("-")
        url = f"{cls._base_url}/coins/{coin_id}/tickers"
        params = {"exchange_ids": exchange_id}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status != 200:
                        body = await response.text()
                        cls.logger().warning(
                            f"CoinGecko tickers request failed ({response.status}) for "
                            f"{connector_name}/{trading_pair}: {body[:200]}"
                        )
                        return None
                    data = await response.json()
        except Exception as e:
            cls.logger().warning(
                f"CoinGecko volume fetch error for {connector_name}/{trading_pair}: {e}"
            )
            return None

        tickers = data.get("tickers") or []
        matched_volume: Optional[Decimal] = None
        for ticker in tickers:
            ticker_base = str(ticker.get("base", "")).upper()
            ticker_target = str(ticker.get("target", "")).upper()
            if ticker_base == base and ticker_target == quote:
                converted = (ticker.get("converted_volume") or {}).get("usd")
                if converted is not None:
                    matched_volume = Decimal(str(converted))
                    break

        if matched_volume is None:
            cls.logger().warning(
                f"No CoinGecko ticker match for {trading_pair} on {exchange_id} (coin_id={coin_id})."
            )
            return None

        cls._cache[cache_key] = (matched_volume, now)
        return matched_volume

"""
Uniswap V3 pool reader for DEX/CEX PMM feed.

Primary TWAP: pool.observe([seconds, 0]) → average tick → price.
Fallback spot: slot0().sqrtPriceX96.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

POOL_ABI: List[dict] = [
    {
        "inputs": [{"name": "secondsAgos", "type": "uint32[]"}],
        "name": "observe",
        "outputs": [
            {"name": "tickCumulatives", "type": "int56[]"},
            {"name": "secondsPerLiquidityCumulativeX128s", "type": "uint160[]"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {"inputs": [], "name": "token0", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token1", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

ERC20_ABI: List[dict] = [
    {"inputs": [], "name": "symbol", "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"},
]


def _norm_addr(address: str) -> str:
    return address.lower()


def tick_to_price_0_in_1(tick: int, decimals0: int, decimals1: int) -> Decimal:
    """Human-readable token1 per token0 from tick index."""
    raw = Decimal(str(math.pow(1.0001, tick)))
    return raw * (Decimal(10) ** (decimals0 - decimals1))


def price_0_in_1_from_sqrt(sqrt_price_x96: int, decimals0: int, decimals1: int) -> Decimal:
    """Human-readable token1 per token0 from slot0 sqrtPriceX96."""
    sqrt = Decimal(sqrt_price_x96) / Decimal(2**96)
    ratio = sqrt * sqrt
    return ratio * (Decimal(10) ** (decimals0 - decimals1))


def base_in_quote_from_price_0_in_1(
    price_0_in_1: Decimal,
    token0_address: str,
    token1_address: str,
    base_token_address: str,
    quote_token_address: str,
) -> Decimal:
    """
    Quote token per 1 base token (e.g. WETH per 1 ALI).

    price_0_in_1 is always token1 per token0 in human units.
    """
    base0 = _norm_addr(token0_address) == _norm_addr(base_token_address)
    quote1 = _norm_addr(token1_address) == _norm_addr(quote_token_address)
    if base0 and quote1:
        return price_0_in_1
    quote0 = _norm_addr(token0_address) == _norm_addr(quote_token_address)
    base1 = _norm_addr(token1_address) == _norm_addr(base_token_address)
    if quote0 and base1:
        return Decimal(1) / price_0_in_1
    raise ValueError(
        f"Pool tokens {token0_address}/{token1_address} do not match "
        f"base={base_token_address} quote={quote_token_address}"
    )


@dataclass(frozen=True)
class TokenMeta:
    address: str
    symbol: str
    decimals: int


@dataclass(frozen=True)
class PoolTwapResult:
    base_in_quote: Decimal
    avg_tick: float
    twap_seconds: int
    price_0_in_1: Decimal


@dataclass(frozen=True)
class PoolSpotResult:
    base_in_quote: Decimal
    tick: int
    sqrt_price_x96: int
    price_0_in_1: Decimal


class UniswapV3PoolReader:
    def __init__(
        self,
        rpc_url: str,
        pool_address: str,
        base_token_address: str,
        quote_token_address: str,
        logger: Optional[logging.Logger] = None,
    ):
        from web3 import Web3

        self._logger = logger or logging.getLogger(__name__)
        self._w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
        if not self._w3.is_connected():
            raise ConnectionError(f"RPC not connected: {rpc_url[:64]}...")
        pool_checksum = self._w3.to_checksum_address(pool_address)
        self._pool = self._w3.eth.contract(address=pool_checksum, abi=POOL_ABI)
        self._pool_address = pool_address
        self._base_token_address = base_token_address
        self._quote_token_address = quote_token_address
        self._token0: TokenMeta
        self._token1: TokenMeta
        self._load_tokens()

    def _load_tokens(self) -> None:
        token0_addr = self._pool.functions.token0().call()
        token1_addr = self._pool.functions.token1().call()
        self._token0 = self._read_token_meta(token0_addr)
        self._token1 = self._read_token_meta(token1_addr)
        self._logger.info(
            "Uniswap V3 pool %s: token0=%s (%s) token1=%s (%s)",
            self._pool_address,
            self._token0.symbol,
            self._token0.address,
            self._token1.symbol,
            self._token1.address,
        )

    def _read_token_meta(self, address: str) -> TokenMeta:
        contract = self._w3.eth.contract(address=address, abi=ERC20_ABI)
        symbol = contract.functions.symbol().call()
        decimals = int(contract.functions.decimals().call())
        return TokenMeta(address=address, symbol=symbol, decimals=decimals)

    @property
    def token0(self) -> TokenMeta:
        return self._token0

    @property
    def token1(self) -> TokenMeta:
        return self._token1

    def _to_base_in_quote(self, price_0_in_1: Decimal) -> Decimal:
        return base_in_quote_from_price_0_in_1(
            price_0_in_1,
            self._token0.address,
            self._token1.address,
            self._base_token_address,
            self._quote_token_address,
        )

    def get_twap_base_in_quote(self, twap_seconds: int) -> PoolTwapResult:
        if twap_seconds <= 0:
            raise ValueError("twap_seconds must be positive")
        tick_cumulatives, _ = self._pool.functions.observe([twap_seconds, 0]).call()
        tick_delta = int(tick_cumulatives[1]) - int(tick_cumulatives[0])
        avg_tick = tick_delta / twap_seconds
        price_0_in_1 = tick_to_price_0_in_1_from_avg(avg_tick, self._token0.decimals, self._token1.decimals)
        base_in_quote = self._to_base_in_quote(price_0_in_1)
        return PoolTwapResult(
            base_in_quote=base_in_quote,
            avg_tick=avg_tick,
            twap_seconds=twap_seconds,
            price_0_in_1=price_0_in_1,
        )

    def get_spot_base_in_quote(self) -> PoolSpotResult:
        slot0 = self._pool.functions.slot0().call()
        sqrt_price_x96 = int(slot0[0])
        tick = int(slot0[1])
        price_0_in_1 = price_0_in_1_from_sqrt(
            sqrt_price_x96,
            self._token0.decimals,
            self._token1.decimals,
        )
        base_in_quote = self._to_base_in_quote(price_0_in_1)
        return PoolSpotResult(
            base_in_quote=base_in_quote,
            tick=tick,
            sqrt_price_x96=sqrt_price_x96,
            price_0_in_1=price_0_in_1,
        )


def tick_to_price_0_in_1_from_avg(avg_tick: float, decimals0: int, decimals1: int) -> Decimal:
    """Token1 per token0 from fractional average tick (observe TWAP)."""
    raw = Decimal(str(math.pow(1.0001, avg_tick)))
    return raw * (Decimal(10) ** (decimals0 - decimals1))

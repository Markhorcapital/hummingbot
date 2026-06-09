import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from controllers.market_making.uniswap_v3_pool_reader import (
    PoolSpotResult,
    PoolTwapResult,
    UniswapV3PoolReader,
    base_in_quote_from_price_0_in_1,
    price_0_in_1_from_sqrt,
    tick_to_price_0_in_1,
    tick_to_price_0_in_1_from_avg,
)

ALI = "0x6B0b3a982b4634aC68dD83a4DBF02311cE324181"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
POOL = "0xF260d15e8eBe54D210ef53F5b61Cb46bD9Aa29EE"


class TestUniswapV3PoolMath(unittest.TestCase):
    def test_tick_to_price_0_in_1_tick_zero(self):
        self.assertEqual(tick_to_price_0_in_1(0, 18, 18), Decimal(1))

    def test_price_0_in_1_from_sqrt_known(self):
        # sqrtPriceX96 from phase0 report (~tick -141886)
        sqrt_x96 = 65770084291733822632462111
        p = price_0_in_1_from_sqrt(sqrt_x96, 18, 18)
        self.assertGreater(p, Decimal(0))
        self.assertLess(p, Decimal("0.000001"))

    def test_base_in_quote_ali_token0(self):
        p01 = Decimal("6.891244032824809672812701784E-7")
        q = base_in_quote_from_price_0_in_1(p01, ALI, WETH, ALI, WETH)
        self.assertEqual(q, p01)

    def test_base_in_quote_ali_token1(self):
        p01 = Decimal("144.5")
        q = base_in_quote_from_price_0_in_1(p01, WETH, ALI, ALI, WETH)
        self.assertAlmostEqual(float(q), 1 / 144.5, places=10)

    def test_twap_from_avg_tick_matches_html_style(self):
        avg_tick = -141886.0
        p = tick_to_price_0_in_1_from_avg(avg_tick, 18, 18)
        self.assertGreater(p, Decimal(0))


class TestUniswapV3PoolReaderMocked(unittest.TestCase):
    def _make_reader_with_pool(self, mock_w3):
        with patch("web3.Web3") as mock_web3_cls:
            mock_w3.is_connected.return_value = True
            mock_web3_cls.return_value = mock_w3
            mock_web3_cls.HTTPProvider.return_value = MagicMock()
            mock_web3_cls.to_checksum_address.side_effect = lambda x: x

            reader = UniswapV3PoolReader(
                rpc_url="http://localhost:8545",
                pool_address=POOL,
                base_token_address=ALI,
                quote_token_address=WETH,
            )
        return reader

    def test_get_twap_base_in_quote(self):
        mock_w3 = MagicMock()
        mock_pool = MagicMock()
        mock_t0 = MagicMock()
        mock_t1 = MagicMock()

        mock_w3.eth.contract.side_effect = lambda address, abi: (
            mock_pool if address == POOL else (mock_t0 if "6B0b" in str(address) else mock_t1)
        )
        mock_pool.functions.token0.return_value.call.return_value = ALI
        mock_pool.functions.token1.return_value.call.return_value = WETH
        mock_t0.functions.symbol.return_value.call.return_value = "ALI"
        mock_t0.functions.decimals.return_value.call.return_value = 18
        mock_t1.functions.symbol.return_value.call.return_value = "WETH"
        mock_t1.functions.decimals.return_value.call.return_value = 18

        # observe([T,0]): [0]=cumulative at T ago, [1]=now; avg_tick = (now - then) / T
        tick_then = 141886 * 180
        tick_now = 0
        mock_pool.functions.observe.return_value.call.return_value = (
            [tick_then, tick_now],
            [0, 0],
        )

        reader = self._make_reader_with_pool(mock_w3)
        reader._pool = mock_pool
        reader._token0 = reader._read_token_meta = MagicMock  # noqa — set directly
        from controllers.market_making.uniswap_v3_pool_reader import TokenMeta

        reader._token0 = TokenMeta(ALI, "ALI", 18)
        reader._token1 = TokenMeta(WETH, "WETH", 18)

        result = reader.get_twap_base_in_quote(180)
        self.assertIsInstance(result, PoolTwapResult)
        self.assertEqual(result.twap_seconds, 180)
        self.assertEqual(result.avg_tick, -141886.0)
        self.assertGreater(result.base_in_quote, Decimal(0))

    def test_get_spot_base_in_quote(self):
        mock_w3 = MagicMock()
        mock_pool = MagicMock()
        mock_w3.eth.contract.return_value = mock_pool
        mock_pool.functions.token0.return_value.call.return_value = ALI
        mock_pool.functions.token1.return_value.call.return_value = WETH

        reader = self._make_reader_with_pool(mock_w3)
        from controllers.market_making.uniswap_v3_pool_reader import TokenMeta

        reader._pool = mock_pool
        reader._token0 = TokenMeta(ALI, "ALI", 18)
        reader._token1 = TokenMeta(WETH, "WETH", 18)

        sqrt_x96 = 65770084291733822632462111
        mock_pool.functions.slot0.return_value.call.return_value = (
            sqrt_x96,
            -141886,
            0,
            1,
            1,
            0,
            True,
        )
        result = reader.get_spot_base_in_quote()
        self.assertIsInstance(result, PoolSpotResult)
        self.assertEqual(result.tick, -141886)
        self.assertGreater(result.base_in_quote, Decimal(0))


if __name__ == "__main__":
    unittest.main()

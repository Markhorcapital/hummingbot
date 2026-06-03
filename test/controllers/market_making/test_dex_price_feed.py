import time
import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from controllers.market_making.dex_price_feed import DexPriceFeed, DexPriceFeedConfig, TwapSource, compute_basis_pct
from controllers.market_making.uniswap_v3_pool_reader import PoolSpotResult, PoolTwapResult

ALI = "0x6B0b3a982b4634aC68dD83a4DBF02311cE324181"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
POOL = "0xF260d15e8eBe54D210ef53F5b61Cb46bD9Aa29EE"


class TestComputeBasisPct(unittest.TestCase):
    def test_basis_between_polls(self):
        cex = Decimal("0.001435")
        dex = Decimal("0.00146")
        basis = compute_basis_pct(cex, dex)
        self.assertIsNotNone(basis)
        self.assertAlmostEqual(float(basis), -0.0171, places=3)

    def test_basis_none_when_no_dex(self):
        self.assertIsNone(compute_basis_pct(Decimal("1"), None))


class TestDexPriceFeed(unittest.TestCase):
    def setUp(self):
        self.config = DexPriceFeedConfig(
            dex_rpc_url="http://localhost:8545",
            dex_pool_address=POOL,
            base_token_address=ALI,
            quote_token_address=WETH,
            connector_name="mexc",
            dex_eth_usdt_trading_pair="ETH-USDT",
            dex_twap_seconds=60,
            dex_poll_interval_seconds=2,
            dex_price_max_stale_seconds=30,
            dex_sanity_max_divergence_pct=Decimal("0.15"),
        )
        self.mdp = MagicMock()
        self.mdp.get_price_by_type.return_value = Decimal("2000")

        self.pool_reader = MagicMock()
        self.feed = DexPriceFeed(
            config=self.config,
            market_data_provider=self.mdp,
            pool_reader=self.pool_reader,
        )

    def test_poll_observe_path(self):
        self.pool_reader.get_twap_base_in_quote.return_value = PoolTwapResult(
            base_in_quote=Decimal("0.0000005"),
            avg_tick=-140000.0,
            twap_seconds=60,
            price_0_in_1=Decimal("0.0000005"),
        )
        self.pool_reader.get_spot_base_in_quote.return_value = PoolSpotResult(
            base_in_quote=Decimal("0.0000005"),
            tick=-140000,
            sqrt_price_x96=1,
            price_0_in_1=Decimal("0.0000005"),
        )

        snap = self.feed.poll(cex_mid=Decimal("1.0"))
        self.assertEqual(snap.twap_source, TwapSource.OBSERVE)
        self.assertEqual(snap.dex_fair, Decimal("0.0000005") * Decimal("2000"))
        self.assertEqual(snap.eth_usdt_mid, Decimal("2000"))
        self.assertFalse(self.feed.is_stale())

    def test_poll_fallback_when_observe_fails(self):
        self.pool_reader.get_twap_base_in_quote.side_effect = Exception("observe revert")
        self.pool_reader.get_spot_base_in_quote.return_value = PoolSpotResult(
            base_in_quote=Decimal("0.0000005"),
            tick=-140000,
            sqrt_price_x96=1,
            price_0_in_1=Decimal("0.0000005"),
        )
        self.feed.poll()
        snap = self.feed.poll(cex_mid=Decimal("1.0"))
        self.assertEqual(snap.twap_source, TwapSource.FALLBACK)
        self.assertIsNotNone(snap.dex_fair)

    def test_is_stale_after_gap(self):
        self.pool_reader.get_twap_base_in_quote.return_value = PoolTwapResult(
            base_in_quote=Decimal("0.0000005"),
            avg_tick=-140000.0,
            twap_seconds=60,
            price_0_in_1=Decimal("0.0000005"),
        )
        self.pool_reader.get_spot_base_in_quote.return_value = PoolSpotResult(
            base_in_quote=Decimal("0.0000005"),
            tick=-140000,
            sqrt_price_x96=1,
            price_0_in_1=Decimal("0.0000005"),
        )
        self.feed.poll()
        stale_time = time.time() + 31
        self.assertTrue(self.feed.is_stale(now=stale_time))

    def test_sanity_ok_and_fail(self):
        self.pool_reader.get_twap_base_in_quote.return_value = PoolTwapResult(
            base_in_quote=Decimal("0.0005"),
            avg_tick=-140000.0,
            twap_seconds=60,
            price_0_in_1=Decimal("0.0005"),
        )
        self.pool_reader.get_spot_base_in_quote.return_value = PoolSpotResult(
            base_in_quote=Decimal("0.0005"),
            tick=-140000,
            sqrt_price_x96=1,
            price_0_in_1=Decimal("0.0005"),
        )
        self.feed.poll()
        # dex_fair = 0.0005 * 2000 = 1.0
        self.assertTrue(self.feed.sanity_ok(Decimal("1.04")))
        self.assertFalse(self.feed.sanity_ok(Decimal("1.30")))

    def test_basis_pct(self):
        self.pool_reader.get_twap_base_in_quote.return_value = PoolTwapResult(
            base_in_quote=Decimal("0.0005"),
            avg_tick=-140000.0,
            twap_seconds=60,
            price_0_in_1=Decimal("0.0005"),
        )
        self.pool_reader.get_spot_base_in_quote.return_value = PoolSpotResult(
            base_in_quote=Decimal("0.0005"),
            tick=-140000,
            sqrt_price_x96=1,
            price_0_in_1=Decimal("0.0005"),
        )
        snap = self.feed.poll(cex_mid=Decimal("1.05"))
        self.assertIsNotNone(snap.basis_pct)
        self.assertAlmostEqual(float(snap.basis_pct), 0.05, places=6)

    def test_should_poll_interval(self):
        self.assertTrue(self.feed.should_poll())
        self.feed._snapshot.last_poll_ts = time.time()
        self.assertFalse(self.feed.should_poll())


if __name__ == "__main__":
    unittest.main()

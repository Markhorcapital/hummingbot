import os
import unittest
from decimal import Decimal

import yaml

from controllers.market_making.dex_cex_regime import update_regime_with_hysteresis
from controllers.market_making.dex_cex_utils import WarningThrottler
from controllers.market_making.dex_price_feed import compute_basis_pct


class TestDexCexRegime(unittest.TestCase):
    def test_hysteresis_switch_b_to_a(self):
        regime, count = None, 0
        regime, count = update_regime_with_hysteresis(Decimal("0.01"), regime, count, 50, 2)
        self.assertEqual(regime, "A")
        regime, count = update_regime_with_hysteresis(Decimal("-0.01"), regime, count, 50, 2)
        self.assertEqual(regime, "A")
        regime, count = update_regime_with_hysteresis(Decimal("-0.01"), regime, count, 50, 2)
        self.assertEqual(regime, "B")

    def test_dead_band_keeps_regime(self):
        regime, count = "A", 0
        regime, count = update_regime_with_hysteresis(Decimal("0.0001"), regime, count, 50, 2)
        self.assertEqual(regime, "A")
        self.assertEqual(count, 0)

    def test_dead_band_bootstraps_regime_when_unset(self):
        regime, count = update_regime_with_hysteresis(Decimal("0.0001"), None, 0, 50, 2)
        self.assertEqual(regime, "A")
        self.assertEqual(count, 0)
        regime, count = update_regime_with_hysteresis(Decimal("-0.0001"), None, 0, 50, 2)
        self.assertEqual(regime, "B")


class TestWarningThrottler(unittest.TestCase):
    def test_rate_limits(self):
        t = WarningThrottler(interval_seconds=60.0)
        self.assertTrue(t.should_log("k", now=100.0))
        self.assertFalse(t.should_log("k", now=120.0))
        self.assertTrue(t.should_log("k", now=161.0))


class TestBasisRecompute(unittest.TestCase):
    def test_basis_stable_when_dex_unchanged(self):
        cex = Decimal("0.0015")
        dex = Decimal("0.00146")
        b1 = compute_basis_pct(cex, dex)
        cex2 = Decimal("0.00148")
        b2 = compute_basis_pct(cex2, dex)
        self.assertIsNotNone(b1)
        self.assertIsNotNone(b2)
        self.assertNotEqual(b1, b2)


class TestControllerYaml(unittest.TestCase):
    def test_controller_yaml_fields(self):
        path = os.path.join(
            os.path.dirname(__file__),
            "../../../conf/controllers/conf_pmm_dynamic_dex_cex_mexc.yml",
        )
        with open(path) as f:
            data = yaml.safe_load(f)
        self.assertEqual(data["controller_name"], "pmm_dynamic")
        self.assertEqual(data["connector_name"], "mexc")
        self.assertTrue(data["dex_cex_log_only"])
        self.assertTrue(data.get("dex_cex_debug"))
        self.assertEqual(data["buy_amounts_pct"], [1, 1])
        self.assertEqual(data["sell_amounts_pct"], [1, 1])
        self.assertIn("dex_rpc_url", data)
        self.assertTrue(data.get("skip_rebalance"))
        self.assertEqual(data["dex_eth_usdt_trading_pair"], "ETH-USDT")
        self.assertIn("dex_pool_address", data)
        self.assertIn("dex_rpc_url", data)


if __name__ == "__main__":
    unittest.main()

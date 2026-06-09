"""Phase 3 — Regime A/B quoting formulas and YAML gate."""
import os
import unittest
from decimal import Decimal

import yaml

from controllers.market_making.dex_cex_quoting import compute_regime_order_price


class TestRegimeOrderPrice(unittest.TestCase):
    CEX_MID = Decimal("0.00150")
    DEX_FAIR = Decimal("0.00146")

    def test_regime_a_buy_uses_dex_fair(self):
        price = compute_regime_order_price(
            "A", True, Decimal("0.001"), self.CEX_MID, self.DEX_FAIR
        )
        self.assertEqual(price, self.DEX_FAIR * Decimal("0.999"))

    def test_regime_a_sell_uses_cex_mid(self):
        price = compute_regime_order_price(
            "A", False, Decimal("0.005"), self.CEX_MID, self.DEX_FAIR
        )
        self.assertEqual(price, self.CEX_MID * Decimal("1.005"))

    def test_regime_b_buy_uses_cex_mid(self):
        price = compute_regime_order_price(
            "B", True, Decimal("0.001"), self.CEX_MID, self.DEX_FAIR
        )
        self.assertEqual(price, self.CEX_MID * Decimal("0.999"))

    def test_regime_b_sell_uses_dex_fair(self):
        price = compute_regime_order_price(
            "B", False, Decimal("0.005"), self.CEX_MID, self.DEX_FAIR
        )
        self.assertEqual(price, self.DEX_FAIR * Decimal("1.005"))

    def test_level_spreads_scale(self):
        buy_l2 = compute_regime_order_price(
            "A", True, Decimal("0.002"), self.CEX_MID, self.DEX_FAIR
        )
        self.assertEqual(buy_l2, self.DEX_FAIR * Decimal("0.998"))


class TestPhase3Yaml(unittest.TestCase):
    def test_yaml_phase3_fields(self):
        path = os.path.join(
            os.path.dirname(__file__),
            "../../../conf/controllers/conf_pmm_dynamic_dex_cex_mexc.yml",
        )
        with open(path) as f:
            data = yaml.safe_load(f)
        self.assertTrue(data["dex_cex_log_only"])
        self.assertTrue(data.get("dex_cex_debug"))
        self.assertEqual(data["buy_amounts_pct"], [1, 1])
        self.assertEqual(data["sell_amounts_pct"], [1, 1])
        self.assertEqual(len(data["buy_spreads"]), len(data["buy_amounts_pct"]))


if __name__ == "__main__":
    unittest.main()

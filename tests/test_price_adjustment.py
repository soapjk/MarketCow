from __future__ import annotations

import unittest

from pydantic import ValidationError

from marketcow.price_adjustment import PriceAdjustmentContract


FACTOR_PROVENANCE = {
    "factor_applicability": "applicable",
    "corporate_action_factor": "12",
    "factor_source": "tushare",
    "factor_artifact_id": "artifact-1",
    "factor_as_of": "2026-07-25T01:02:03+08:00",
}


class PriceAdjustmentContractTest(unittest.TestCase):
    def test_raw_equity_keeps_factor_but_applies_no_multiplier(self):
        value = PriceAdjustmentContract.model_validate({
            "adjustment": "raw",
            "applied_adjustment_multiplier": "1",
            **FACTOR_PROVENANCE,
        })

        self.assertEqual(value.corporate_action_factor, "12")
        self.assertEqual(value.applied_adjustment_multiplier, "1")
        self.assertEqual(value.factor_as_of, "2026-07-24T17:02:03Z")

    def test_qfq_requires_reference_and_exact_formula(self):
        value = PriceAdjustmentContract.model_validate({
            "adjustment": "qfq",
            "applied_adjustment_multiplier": "0.8",
            "adjustment_reference_date": "2026-07-25",
            "reference_factor": "15",
            **FACTOR_PROVENANCE,
        })
        self.assertEqual(value.adjustment, "qfq")

        with self.assertRaisesRegex(ValidationError, "qfq multiplier"):
            PriceAdjustmentContract.model_validate({
                **value.model_dump(),
                "applied_adjustment_multiplier": "0.9",
            })

    def test_hfq_multiplier_is_the_cumulative_factor(self):
        PriceAdjustmentContract.model_validate({
            "adjustment": "hfq",
            "applied_adjustment_multiplier": "12",
            **FACTOR_PROVENANCE,
        })
        with self.assertRaisesRegex(ValidationError, "hfq multiplier"):
            PriceAdjustmentContract.model_validate({
                "adjustment": "hfq",
                "applied_adjustment_multiplier": "1",
                **FACTOR_PROVENANCE,
            })

    def test_not_applicable_is_raw_without_factor_provenance(self):
        PriceAdjustmentContract.model_validate({
            "adjustment": "raw",
            "factor_applicability": "not_applicable",
            "applied_adjustment_multiplier": "1",
        })
        with self.assertRaisesRegex(ValidationError, "not_applicable"):
            PriceAdjustmentContract.model_validate({
                "adjustment": "raw",
                "factor_applicability": "not_applicable",
                "corporate_action_factor": "1",
                "applied_adjustment_multiplier": "1",
            })

    def test_applicable_factor_requires_complete_provenance(self):
        with self.assertRaisesRegex(ValidationError, "require factor"):
            PriceAdjustmentContract.model_validate({
                "adjustment": "raw",
                "factor_applicability": "applicable",
                "corporate_action_factor": "12",
                "applied_adjustment_multiplier": "1",
            })

    def test_generic_adjusted_is_not_a_canonical_value(self):
        with self.assertRaises(ValidationError):
            PriceAdjustmentContract.model_validate({
                "adjustment": "adjusted",
                "applied_adjustment_multiplier": "1",
                **FACTOR_PROVENANCE,
            })


if __name__ == "__main__":
    unittest.main()

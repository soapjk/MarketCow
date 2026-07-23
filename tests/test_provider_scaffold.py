import unittest

from marketcow.providers.contracts import (
    CapabilityDeclaration,
    DEFAULT_PROVIDER_MANIFESTS,
    ProviderManifest,
    ProviderRegistry,
    REALTIME_QUOTE,
    validate_realtime_quote,
)


class QuoteAdapter:
    def fetch_quote(self, symbol):
        return {"symbol": symbol}


class ProviderScaffoldTest(unittest.TestCase):
    def test_manifest_and_registry_are_the_single_capability_inventory(self):
        registry = ProviderRegistry(DEFAULT_PROVIDER_MANIFESTS)
        self.assertEqual(registry.supported(REALTIME_QUOTE, "CN"), (
            "tushare", "sina", "eastmoney", "longport",
        ))
        self.assertEqual(registry.supported(REALTIME_QUOTE, "US"), ("yahoo", "longport"))

    def test_new_provider_template_binds_only_when_operations_exist(self):
        manifest = ProviderManifest("example", "example_api", (
            CapabilityDeclaration(REALTIME_QUOTE, frozenset({"US"}), "fetch_quote"),
        ))
        registry = ProviderRegistry((manifest,))
        registry.bind("example", QuoteAdapter())
        self.assertIsInstance(registry.get("example"), QuoteAdapter)
        with self.assertRaises(ValueError):
            registry.bind("example", QuoteAdapter())

    def test_missing_declared_operation_fails_at_registration(self):
        manifest = ProviderManifest("example", "example_api", (
            CapabilityDeclaration(REALTIME_QUOTE, frozenset({"US"}), "fetch_quote"),
        ))
        with self.assertRaisesRegex(TypeError, "fetch_quote"):
            ProviderRegistry((manifest,)).bind("example", object())

    def test_binding_can_validate_the_capability_subset_used_by_a_component(self):
        manifest = next(item for item in DEFAULT_PROVIDER_MANIFESTS if item.provider_id == "yahoo")
        registry = ProviderRegistry((manifest,))
        registry.bind("yahoo", QuoteAdapter(), (REALTIME_QUOTE,))
        self.assertIsInstance(registry.get("yahoo"), QuoteAdapter)

    def test_manifest_rejects_unknown_market_and_duplicate_capability(self):
        with self.assertRaises(ValueError):
            CapabilityDeclaration(REALTIME_QUOTE, frozenset({"MARS"}), "fetch_quote")
        capability = CapabilityDeclaration(REALTIME_QUOTE, frozenset({"US"}), "fetch_quote")
        with self.assertRaises(ValueError):
            ProviderManifest("example", "example_api", (capability, capability))

    def test_normalized_quote_contract_is_reusable(self):
        valid = {
            "instrument_id": "US.XNAS.TEST",
            "symbol": "TEST",
            "market": "US",
            "price": 12.5,
            "source": "example_api",
            "source_url": "https://example.invalid/quotes",
            "raw_response_locator": "data[0]",
            "_raw_payload": {"last": 12.5},
        }
        validate_realtime_quote(valid)
        with self.assertRaisesRegex(ValueError, "raw_response_locator"):
            validate_realtime_quote({key: value for key, value in valid.items() if key != "raw_response_locator"})


if __name__ == "__main__":
    unittest.main()

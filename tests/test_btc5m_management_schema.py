"""Machine-check the diagnostic management contract, including forbidden ready=true."""

import json
from pathlib import Path

import pytest

jsonschema = pytest.importorskip("jsonschema")

FIXTURES = Path(__file__).parent / "fixtures"
SCHEMA = json.loads((FIXTURES / "btc-5m-management-health-v1.schema.json").read_text())
VALIDATOR = jsonschema.Draft202012Validator(SCHEMA, format_checker=jsonschema.FormatChecker())


@pytest.mark.parametrize(
    "name",
    [
        "btc-5m-management-health-v1-healthy.json",
        "btc-5m-management-health-v1-stale.json",
        "btc-5m-management-health-v1.json",
        "btc-5m-management-health-v1-manager-missing.json",
    ],
)
def test_complete_management_fixture_matches_schema(name: str) -> None:
    VALIDATOR.validate(json.loads((FIXTURES / name).read_text()))


def test_management_window_never_asserts_ready_true() -> None:
    fixture = json.loads((FIXTURES / "btc-5m-management-health-v1-healthy.json").read_text())
    fixture["windows"][0]["ready"] = True
    with pytest.raises(jsonschema.ValidationError):
        VALIDATOR.validate(fixture)

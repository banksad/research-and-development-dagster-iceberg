from pathlib import Path

import pytest

from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

CONTRACT_PATH = Path("config/table_contracts/base.yaml")


def test_load_base_contracts_yaml():
    contracts = load_table_contracts(CONTRACT_PATH)

    assert contracts
    assert "raw.full_responses" in contracts
    assert "ops.response_corrections" in contracts


def test_all_planned_identifiers_have_contracts():
    contracts = load_table_contracts(CONTRACT_PATH)

    missing = refs.PLANNED_TABLE_IDENTIFIERS - set(contracts)
    assert not missing


def test_correction_contracts_include_required_audit_columns():
    contracts = load_table_contracts(CONTRACT_PATH)

    response_columns = contracts["ops.response_corrections"].required_columns
    postcode_columns = contracts["ops.postcode_corrections"].required_columns

    expected_response = {
        "correction_id",
        "survey_year",
        "survey_type",
        "reference",
        "instance",
        "variable",
        "old_value",
        "new_value",
        "reason",
        "source",
        "approved_by",
        "approved_at",
        "created_at",
    }

    expected_postcode = {
        "correction_id",
        "survey_year",
        "survey_type",
        "reference",
        "instance",
        "old_postcode",
        "new_postcode",
        "reason",
        "source",
        "approved_by",
        "approved_at",
        "created_at",
    }

    assert expected_response.issubset(response_columns)
    assert expected_postcode.issubset(postcode_columns)


def test_no_freezing_or_construction_contract_identifiers():
    contracts = load_table_contracts(CONTRACT_PATH)

    forbidden = ("freezing", "construction")
    for identifier in contracts:
        assert all(token not in identifier for token in forbidden)


def test_get_table_contract_raises_on_unknown_identifier():
    contracts = load_table_contracts(CONTRACT_PATH)

    with pytest.raises(KeyError, match="Unknown table contract identifier"):
        get_table_contract(contracts, "ops.unknown_corrections")

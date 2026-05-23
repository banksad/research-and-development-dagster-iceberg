from __future__ import annotations

import pandas as pd

from src.randd_pipeline.checks.table_checks import (
    check_contract_required_columns,
    check_non_empty,
    check_required_columns,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts


def test_check_required_columns_passes_when_all_columns_present() -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})

    passed, message = check_required_columns(df, ["a", "b"])

    assert passed
    assert "All required columns present" in message


def test_check_required_columns_fails_with_clear_missing_columns_message() -> None:
    df = pd.DataFrame({"a": [1]})

    passed, message = check_required_columns(df, ["a", "b", "c"])

    assert not passed
    assert message == "Missing required columns: b, c"


def test_check_non_empty_passes_and_fails_as_expected() -> None:
    non_empty_df = pd.DataFrame({"a": [1]})
    empty_df = pd.DataFrame({"a": []})

    passed_non_empty, non_empty_message = check_non_empty(non_empty_df)
    passed_empty, empty_message = check_non_empty(empty_df)

    assert passed_non_empty
    assert non_empty_message == "Table has 1 rows."
    assert not passed_empty
    assert empty_message == "Table is empty (0 rows)."


def test_check_contract_required_columns_uses_raw_full_responses_contract() -> None:
    contracts = load_table_contracts("config/table_contracts/base.yaml")
    contract = get_table_contract(contracts, refs.RAW_FULL_RESPONSES)

    valid_df = pd.DataFrame(
        {
            "survey_year": [2099],
            "survey_type": ["SYNTH"],
            "reference": ["R1"],
            "instance": [1],
        }
    )
    invalid_df = valid_df.drop(columns=["instance"])

    valid_passed, valid_message = check_contract_required_columns(valid_df, contract)
    invalid_passed, invalid_message = check_contract_required_columns(invalid_df, contract)

    assert valid_passed
    assert refs.RAW_FULL_RESPONSES in valid_message
    assert not invalid_passed
    assert invalid_message == "raw.full_responses: Missing required columns: instance"

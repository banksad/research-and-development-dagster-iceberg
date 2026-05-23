from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.mapping_checks import (
    check_cell_number_mapped_responses_mapping_columns_present,
    check_cell_number_mapped_responses_non_empty,
    check_cell_number_mapped_responses_required_columns,
    check_cell_number_mapped_responses_unique_grain,
)
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

SCENARIO_ID = "mapping_cell_number_minimal"


def _mapped_df() -> pd.DataFrame:
    return load_scenario_csv(SCENARIO_ID, "expected_cell_number_mapped_responses.csv")


def test_required_columns_pass_for_expected_fixture() -> None:
    assert check_cell_number_mapped_responses_required_columns(_mapped_df())[0]


def test_required_columns_fail_clearly_when_missing() -> None:
    passed, message = check_cell_number_mapped_responses_required_columns(_mapped_df().drop(columns=["cellnumber"]))
    assert not passed
    assert "Missing required columns: cellnumber" in message


def test_non_empty_passes_and_fails() -> None:
    assert check_cell_number_mapped_responses_non_empty(_mapped_df())[0]
    assert not check_cell_number_mapped_responses_non_empty(_mapped_df().iloc[0:0].copy())[0]


def test_unique_grain_passes_and_fails() -> None:
    df = _mapped_df()
    assert check_cell_number_mapped_responses_unique_grain(df)[0]
    dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    passed, message = check_cell_number_mapped_responses_unique_grain(dup)
    assert not passed
    assert "duplicate row(s)" in message


def test_mapping_columns_present_passes_for_expected_fixture() -> None:
    assert check_cell_number_mapped_responses_mapping_columns_present(_mapped_df())[0]


def test_mapping_columns_allow_null_cellno_rows() -> None:
    df = _mapped_df().copy()
    df.loc[0, ["cellno", "cellnumber", "uni_count", "uni_employment"]] = [None, None, None, None]
    assert check_cell_number_mapped_responses_mapping_columns_present(df)[0]


@pytest.mark.parametrize("column", ["cellnumber", "uni_count", "uni_employment"])
def test_mapping_columns_fail_when_non_null_cellno_has_missing_metadata(column: str) -> None:
    df = _mapped_df().copy()
    df.loc[0, "cellno"] = 1
    df.loc[0, column] = None
    passed, message = check_cell_number_mapped_responses_mapping_columns_present(df)
    assert not passed
    assert "non-null cellno" in message

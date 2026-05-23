from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.mapping_checks import (
    check_cell_number_mapped_responses_mapping_columns_present,
    check_cell_number_mapped_responses_non_empty,
    check_cell_number_mapped_responses_required_columns,
    check_cell_number_mapped_responses_unique_grain,
    check_mapped_responses_non_empty,
    check_mapped_responses_required_columns,
    check_mapped_responses_ultfoc_present,
    check_mapped_responses_unique_grain,
    check_cell_number_mapped_responses_mapping_columns_present,
)
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def _mapped_df() -> pd.DataFrame:
    return load_scenario_csv("mapping_cell_number_minimal", "expected_cell_number_mapped_responses.csv")


def _cell_number_mapped_df() -> pd.DataFrame:
    return load_scenario_csv("mapping_cell_number_minimal", "expected_cell_number_mapped_responses.csv")


def test_mapped_responses_required_columns_pass_for_canonical_v1_fixture() -> None:
    assert check_mapped_responses_required_columns(_mapped_df())[0]


@pytest.mark.parametrize("column", ["survey_year", "survey_type"])
def test_mapped_responses_required_columns_fail_clearly_when_missing(column: str) -> None:
    passed, message = check_mapped_responses_required_columns(_mapped_df().drop(columns=[column]))
    assert not passed
    assert f"Missing required columns: {column}" in message


def test_mapped_responses_non_empty_passes_and_fails() -> None:
    assert check_mapped_responses_non_empty(_mapped_df())[0]
    assert not check_mapped_responses_non_empty(_mapped_df().iloc[0:0].copy())[0]


def test_mapped_responses_unique_grain_passes_and_fails() -> None:
    df = _mapped_df()
    assert check_mapped_responses_unique_grain(df)[0]
    dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    passed, message = check_mapped_responses_unique_grain(dup)
    assert not passed
    assert "duplicate row(s)" in message


def test_mapped_responses_ultfoc_present_passes() -> None:
    assert check_mapped_responses_ultfoc_present(_mapped_df())[0]


@pytest.mark.parametrize("bad_value", [None, "   "])
def test_mapped_responses_ultfoc_present_fails_for_null_or_blank_values(bad_value: object) -> None:
    df = _mapped_df().copy()
    df.loc[0, "ultfoc"] = bad_value
    passed, message = check_mapped_responses_ultfoc_present(df)
    assert not passed
    assert "null/blank" in message


def test_mapped_responses_ultfoc_present_fails_when_column_missing() -> None:
    passed, message = check_mapped_responses_ultfoc_present(_mapped_df().drop(columns=["ultfoc"]))
    assert not passed
    assert "missing column: ultfoc" in message


def test_required_columns_pass_for_expected_fixture() -> None:
    assert check_cell_number_mapped_responses_required_columns(_cell_number_mapped_df())[0]


def test_required_columns_fail_clearly_when_missing() -> None:
    passed, message = check_cell_number_mapped_responses_required_columns(_cell_number_mapped_df().drop(columns=["cellnumber"]))
    assert not passed
    assert "Missing required columns: cellnumber" in message


def test_non_empty_passes_and_fails() -> None:
    assert check_cell_number_mapped_responses_non_empty(_cell_number_mapped_df())[0]
    assert not check_cell_number_mapped_responses_non_empty(_cell_number_mapped_df().iloc[0:0].copy())[0]


def test_unique_grain_passes_and_fails() -> None:
    df = _cell_number_mapped_df()
    assert check_cell_number_mapped_responses_unique_grain(df)[0]
    dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    passed, message = check_cell_number_mapped_responses_unique_grain(dup)
    assert not passed
    assert "duplicate row(s)" in message


def test_mapping_columns_present_passes_for_expected_fixture() -> None:
    assert check_cell_number_mapped_responses_mapping_columns_present(_cell_number_mapped_df())[0]


def test_mapping_columns_allow_null_cellno_rows() -> None:
    df = _cell_number_mapped_df().copy()
    df.loc[0, ["cellno", "cellnumber", "uni_count", "uni_employment"]] = [None, None, None, None]
    assert check_cell_number_mapped_responses_mapping_columns_present(df)[0]


@pytest.mark.parametrize("column", ["cellnumber", "uni_count", "uni_employment"])
def test_mapping_columns_fail_when_non_null_cellno_has_null_metadata(column: str) -> None:
    df = _cell_number_mapped_df().copy()
    df.loc[0, "cellno"] = 1
    df.loc[0, column] = None
    passed, message = check_cell_number_mapped_responses_mapping_columns_present(df)
    assert not passed
    assert "non-null cellno" in message


@pytest.mark.parametrize("column", ["cellnumber", "uni_count", "uni_employment"])
def test_mapping_columns_fail_when_non_null_cellno_has_blank_metadata(column: str) -> None:
    df = _cell_number_mapped_df().copy()
    df.loc[0, "cellno"] = 1
    df.loc[0, column] = "  "
    passed, message = check_cell_number_mapped_responses_mapping_columns_present(df)
    assert not passed
    assert "non-null cellno" in message


def test_mapping_checks_module_does_not_import_legacy_modules() -> None:
    importlib.import_module("src.randd_pipeline.checks.mapping_checks")
    banned_prefixes = ["src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]
    assert not any(
        name == prefix or name.startswith(f"{prefix}.")
        for name in importlib.sys.modules
        for prefix in banned_prefixes
    )


def test_mapped_responses_cell_number_metadata_completeness_passes() -> None:
    assert check_cell_number_mapped_responses_mapping_columns_present(_mapped_df())[0]


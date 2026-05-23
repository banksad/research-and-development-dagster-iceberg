from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.mapping_checks import (
    check_mapped_responses_non_empty,
    check_mapped_responses_required_columns,
    check_mapped_responses_ultfoc_present,
    check_mapped_responses_unique_grain,
)
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

SCENARIO_ID = "mapping_foreign_ownership_minimal"


def _mapped_df() -> pd.DataFrame:
    return load_scenario_csv(SCENARIO_ID, "expected_mapped_responses.csv")


def test_mapped_required_columns_check_passes_for_expected_output() -> None:
    passed, message = check_mapped_responses_required_columns(_mapped_df())
    assert passed
    assert "intermediate.mapped_responses" in message


def test_mapped_required_columns_check_fails_clearly_when_survey_columns_missing() -> None:
    df = _mapped_df().drop(columns=["survey_year", "survey_type"])
    passed, message = check_mapped_responses_required_columns(df)
    assert not passed
    assert "Missing required columns: survey_year, survey_type" in message


def test_mapped_non_empty_check_passes_and_fails_clearly() -> None:
    passed, message = check_mapped_responses_non_empty(_mapped_df())
    assert passed
    assert "Table has" in message

    failed, fail_message = check_mapped_responses_non_empty(_mapped_df().iloc[0:0].copy())
    assert not failed
    assert "Table is empty" in fail_message


def test_mapped_unique_grain_check_passes_for_expected_output() -> None:
    passed, message = check_mapped_responses_unique_grain(_mapped_df())
    assert passed
    assert "Mapped grain is unique" in message


def test_mapped_unique_grain_check_fails_clearly_for_duplicate_rows() -> None:
    df = _mapped_df()
    duplicate_df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    passed, message = check_mapped_responses_unique_grain(duplicate_df)
    assert not passed
    assert "duplicate row(s)" in message


def test_mapped_ultfoc_check_passes_for_expected_output() -> None:
    passed, message = check_mapped_responses_ultfoc_present(_mapped_df())
    assert passed
    assert "ultfoc is populated" in message


def test_mapped_ultfoc_check_fails_for_missing_null_and_blank_values() -> None:
    missing_col_df = _mapped_df().drop(columns=["ultfoc"])
    passed_missing, message_missing = check_mapped_responses_ultfoc_present(missing_col_df)
    assert not passed_missing
    assert "missing column: ultfoc" in message_missing

    null_df = _mapped_df().copy()
    null_df.loc[0, "ultfoc"] = None
    passed_null, message_null = check_mapped_responses_ultfoc_present(null_df)
    assert not passed_null
    assert "null/blank values" in message_null

    blank_df = _mapped_df().copy()
    blank_df.loc[0, "ultfoc"] = "   "
    passed_blank, message_blank = check_mapped_responses_ultfoc_present(blank_df)
    assert not passed_blank
    assert "null/blank values" in message_blank

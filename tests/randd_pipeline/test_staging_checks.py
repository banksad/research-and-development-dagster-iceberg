from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.staging_checks import (
    check_staged_responses_non_empty,
    check_staged_responses_required_columns,
    check_staged_responses_unique_grain,
)
from src.randd_pipeline.domain.staging.transmutation import build_full_responses
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

SCENARIO_ID = "staging_minimal_valid_responses"


def _build_staged_df() -> pd.DataFrame:
    contributors = load_scenario_csv(SCENARIO_ID, "contributors.csv")
    responses = load_scenario_csv(SCENARIO_ID, "responses_long.csv")
    return build_full_responses(contributors=contributors, responses=responses)


def test_staged_required_columns_check_passes_for_transmutation_output() -> None:
    df = _build_staged_df()
    passed, message = check_staged_responses_required_columns(df)
    assert passed
    assert "intermediate.staged_responses" in message


def test_staged_required_columns_check_fails_clearly_when_survey_columns_missing() -> None:
    df = _build_staged_df().drop(columns=["survey_year", "survey_type"])
    passed, message = check_staged_responses_required_columns(df)
    assert not passed
    assert "Missing required columns: survey_year, survey_type" in message


def test_staged_non_empty_check_passes_and_fails_clearly() -> None:
    passed, message = check_staged_responses_non_empty(_build_staged_df())
    assert passed
    assert "Table has" in message

    empty_df = _build_staged_df().iloc[0:0].copy()
    failed, fail_message = check_staged_responses_non_empty(empty_df)
    assert not failed
    assert "Table is empty" in fail_message


def test_staged_unique_grain_check_passes_for_expected_output() -> None:
    passed, message = check_staged_responses_unique_grain(_build_staged_df())
    assert passed
    assert "Staged grain is unique" in message


def test_staged_unique_grain_check_fails_clearly_for_duplicate_rows() -> None:
    df = _build_staged_df()
    duplicate_df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    passed, message = check_staged_responses_unique_grain(duplicate_df)
    assert not passed
    assert "duplicate row(s)" in message

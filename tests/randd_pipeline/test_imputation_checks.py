from __future__ import annotations

import pytest

pytest.importorskip("pandas")
pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.imputation_checks import (
    check_imputation_marker_populated,
    check_imputed_responses_non_empty,
    check_imputed_responses_required_columns,
    check_imputed_responses_unique_grain,
    check_no_illegal_missing_imputed_values,
)
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

SCENARIO = "mapping_to_imputation_minimal"


def _expected_df():
    return load_scenario_csv(SCENARIO, "expected_imputed_responses.csv")


def test_check_imputed_responses_required_columns_passes_on_expected_fixture() -> None:
    ok, msg = check_imputed_responses_required_columns(_expected_df())
    assert ok, msg


def test_check_imputed_responses_required_columns_fails_when_imp_marker_missing() -> None:
    ok, msg = check_imputed_responses_required_columns(_expected_df().drop(columns=["imp_marker"]))
    assert not ok
    assert "imp_marker" in msg


def test_check_imputed_responses_required_columns_fails_when_601_imputed_missing() -> None:
    ok, msg = check_imputed_responses_required_columns(_expected_df().drop(columns=["601_imputed"]))
    assert not ok
    assert "601_imputed" in msg


def test_check_imputed_responses_required_columns_fails_when_grain_column_missing() -> None:
    ok, msg = check_imputed_responses_required_columns(_expected_df().drop(columns=["survey_year"]))
    assert not ok
    assert "survey_year" in msg


def test_check_imputed_responses_non_empty_passes_on_expected_fixture() -> None:
    ok, msg = check_imputed_responses_non_empty(_expected_df())
    assert ok, msg


def test_check_imputed_responses_non_empty_fails_on_empty_df() -> None:
    ok, msg = check_imputed_responses_non_empty(_expected_df().iloc[0:0])
    assert not ok
    assert "empty" in msg.lower()


def test_check_imputed_responses_unique_grain_passes_on_expected_fixture() -> None:
    ok, msg = check_imputed_responses_unique_grain(_expected_df())
    assert ok, msg


def test_check_imputed_responses_unique_grain_fails_on_duplicate_grain() -> None:
    df = _expected_df()
    duped = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    ok, msg = check_imputed_responses_unique_grain(duped)
    assert not ok
    assert "duplicate" in msg.lower()


def test_check_imputation_marker_populated_passes_on_expected_fixture() -> None:
    ok, msg = check_imputation_marker_populated(_expected_df())
    assert ok, msg


def test_check_imputation_marker_populated_fails_for_null_marker() -> None:
    df = _expected_df()
    df.loc[df.index[0], "imp_marker"] = None
    ok, msg = check_imputation_marker_populated(df)
    assert not ok
    assert "imp_marker" in msg


def test_check_imputation_marker_populated_fails_for_blank_marker() -> None:
    df = _expected_df()
    df.loc[df.index[0], "imp_marker"] = "   "
    ok, msg = check_imputation_marker_populated(df)
    assert not ok
    assert "imp_marker" in msg


def test_check_no_illegal_missing_imputed_values_passes_on_expected_fixture() -> None:
    ok, msg = check_no_illegal_missing_imputed_values(_expected_df())
    assert ok, msg


def test_check_no_illegal_missing_imputed_values_allows_missing_for_no_mean_found_only() -> None:
    df = _expected_df()
    df.loc[df.index[0], "imp_marker"] = "no_mean_found"
    df.loc[df.index[0], "601_imputed"] = None
    ok, msg = check_no_illegal_missing_imputed_values(df)
    assert ok, msg


def test_check_no_illegal_missing_imputed_values_fails_for_tmi_missing_imputed() -> None:
    df = _expected_df()
    df.loc[df.index[0], "imp_marker"] = "TMI"
    df.loc[df.index[0], "601_imputed"] = None
    ok, msg = check_no_illegal_missing_imputed_values(df)
    assert not ok
    assert "illegal missing" in msg.lower()


def test_check_no_illegal_missing_imputed_values_fails_for_not_imputed_missing_imputed() -> None:
    df = _expected_df()
    df.loc[df.index[0], "imp_marker"] = "not_imputed"
    df.loc[df.index[0], "601_imputed"] = None
    ok, msg = check_no_illegal_missing_imputed_values(df)
    assert not ok
    assert "illegal missing" in msg.lower()


def test_check_no_illegal_missing_imputed_values_fails_clearly_when_required_columns_missing() -> None:
    ok, msg = check_no_illegal_missing_imputed_values(_expected_df().drop(columns=["imp_marker"]))
    assert not ok
    assert "requires columns" in msg

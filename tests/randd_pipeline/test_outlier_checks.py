from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.outlier_checks import (
    check_manual_adjustment_reason_present,
    check_outlier_adjusted_non_empty,
    check_outlier_adjusted_required_columns,
    check_outlier_adjusted_unique_grain,
    check_outlier_flag_populated,
    check_outlier_source_populated,
)
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def _expected_df():
    return load_scenario_csv("imputation_to_outlier_minimal", "expected_outlier_adjusted_responses.csv")


def test_check_outlier_adjusted_required_columns_passes_on_expected_fixture() -> None:
    passed, _ = check_outlier_adjusted_required_columns(_expected_df())
    assert passed


def test_check_outlier_adjusted_required_columns_fails_when_outlier_missing() -> None:
    df = _expected_df().drop(columns=["outlier"])
    passed, message = check_outlier_adjusted_required_columns(df)
    assert not passed
    assert "outlier" in message


def test_check_outlier_adjusted_required_columns_fails_when_outlier_source_missing() -> None:
    df = _expected_df().drop(columns=["outlier_source"])
    passed, message = check_outlier_adjusted_required_columns(df)
    assert not passed
    assert "outlier_source" in message


def test_check_outlier_adjusted_required_columns_fails_when_grain_column_missing() -> None:
    df = _expected_df().drop(columns=["instance"])
    passed, message = check_outlier_adjusted_required_columns(df)
    assert not passed
    assert "instance" in message


def test_check_outlier_adjusted_non_empty_passes_on_expected_fixture() -> None:
    passed, _ = check_outlier_adjusted_non_empty(_expected_df())
    assert passed


def test_check_outlier_adjusted_non_empty_fails_on_empty_dataframe() -> None:
    df = _expected_df().iloc[0:0]
    passed, _ = check_outlier_adjusted_non_empty(df)
    assert not passed


def test_check_outlier_adjusted_unique_grain_passes_on_expected_fixture() -> None:
    passed, _ = check_outlier_adjusted_unique_grain(_expected_df())
    assert passed


def test_check_outlier_adjusted_unique_grain_fails_on_duplicate_grain() -> None:
    df = _expected_df()
    duplicated = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    passed, message = check_outlier_adjusted_unique_grain(duplicated)
    assert not passed
    assert "duplicate" in message.lower()


def test_check_outlier_flag_populated_passes_on_expected_fixture() -> None:
    passed, _ = check_outlier_flag_populated(_expected_df())
    assert passed


def test_check_outlier_flag_populated_fails_when_outlier_null() -> None:
    df = _expected_df()
    df.loc[0, "outlier"] = None
    passed, message = check_outlier_flag_populated(df)
    assert not passed
    assert "null" in message.lower()


def test_check_outlier_source_populated_passes_on_expected_fixture() -> None:
    passed, _ = check_outlier_source_populated(_expected_df())
    assert passed


def test_check_outlier_source_populated_fails_when_outlier_source_null() -> None:
    df = _expected_df()
    df.loc[0, "outlier_source"] = None
    passed, message = check_outlier_source_populated(df)
    assert not passed
    assert "blank/null" in message.lower()


def test_check_outlier_source_populated_fails_when_outlier_source_blank() -> None:
    df = _expected_df()
    df.loc[0, "outlier_source"] = "   "
    passed, message = check_outlier_source_populated(df)
    assert not passed
    assert "blank/null" in message.lower()


def test_check_outlier_source_populated_fails_when_outlier_source_invalid() -> None:
    df = _expected_df()
    df.loc[0, "outlier_source"] = "bad"
    passed, message = check_outlier_source_populated(df)
    assert not passed
    assert "invalid" in message.lower()


def test_check_manual_adjustment_reason_present_passes_on_expected_fixture() -> None:
    passed, _ = check_manual_adjustment_reason_present(_expected_df())
    assert passed


def test_check_manual_adjustment_reason_present_allows_blank_or_null_when_not_adjusted() -> None:
    df = _expected_df()
    mask = ~df["outlier_adjustment_applied"]
    df.loc[mask, "outlier_reason"] = None
    passed, _ = check_manual_adjustment_reason_present(df)
    assert passed


def test_check_manual_adjustment_reason_present_fails_when_adjusted_reason_null() -> None:
    df = _expected_df()
    idx = df.index[df["outlier_adjustment_applied"]][0]
    df.loc[idx, "outlier_reason"] = None
    passed, message = check_manual_adjustment_reason_present(df)
    assert not passed
    assert "outlier_reason" in message


def test_check_manual_adjustment_reason_present_fails_when_adjusted_reason_blank() -> None:
    df = _expected_df()
    idx = df.index[df["outlier_adjustment_applied"]][0]
    df.loc[idx, "outlier_reason"] = "   "
    passed, message = check_manual_adjustment_reason_present(df)
    assert not passed
    assert "outlier_reason" in message


def test_check_manual_adjustment_reason_present_fails_clearly_when_required_columns_missing() -> None:
    df = _expected_df().drop(columns=["outlier_reason"])
    passed, message = check_manual_adjustment_reason_present(df)
    assert not passed
    assert "requires columns" in message.lower()

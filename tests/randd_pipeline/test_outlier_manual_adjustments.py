from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.domain.outliers import apply_manual_outlier_adjustments
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def _imputed_df():
    return load_scenario_csv("imputation_to_outlier_minimal", "input_imputed_responses.csv")


def _manual_df():
    return load_scenario_csv("imputation_to_outlier_minimal", "input_ops_manual_outliers.csv")


def test_expected_fixture_output() -> None:
    expected = load_scenario_csv("imputation_to_outlier_minimal", "expected_outlier_adjusted_responses.csv")
    actual = apply_manual_outlier_adjustments(_imputed_df(), _manual_df())
    assert_frame_equal_sorted(actual, expected, sort_by=["reference", "instance", "survey_type", "survey_year"])


def test_no_manual_rows_defaults_correctly() -> None:
    out = apply_manual_outlier_adjustments(_imputed_df(), pd.DataFrame())
    assert (out["outlier_source"] == ["default_none", "default_none", "auto", "auto"]).all()


def test_existing_auto_outlier_true_respected_when_no_manual_row_exists() -> None:
    out = apply_manual_outlier_adjustments(_imputed_df(), pd.DataFrame())
    row = out.loc[out["reference"] == "SYN003"].iloc[0]
    assert bool(row["outlier"]) is True
    assert row["outlier_source"] == "auto"


def test_manual_true_overrides_base_default_false() -> None:
    imputed = _imputed_df().drop(columns=["auto_outlier"])
    manual = pd.DataFrame(
        [{"survey_year": 2024, "survey_type": "RD", "reference": "SYN001", "instance": 1, "manual_outlier": True, "outlier_reason": "Manual reviewed"}]
    )
    out = apply_manual_outlier_adjustments(imputed, manual, default_outlier=False)
    row = out.loc[out["reference"] == "SYN001"].iloc[0]
    assert bool(row["outlier"]) is True
    assert row["outlier_source"] == "manual_outlier"


def test_manual_false_overrides_existing_auto_outlier_true() -> None:
    manual = pd.DataFrame(
        [{"survey_year": 2024, "survey_type": "RD", "reference": "SYN003", "instance": 1, "manual_outlier": False, "outlier_reason": "Reclassified"}]
    )
    out = apply_manual_outlier_adjustments(_imputed_df(), manual)
    row = out.loc[out["reference"] == "SYN003"].iloc[0]
    assert bool(row["outlier"]) is False
    assert row["outlier_source"] == "manual_outlier"


def test_duplicate_manual_response_grain_fails_clearly() -> None:
    manual = pd.concat([_manual_df(), _manual_df().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate response grain"):
        apply_manual_outlier_adjustments(_imputed_df(), manual, strict_manual_references=False)


def test_unmatched_manual_row_fails_when_strict_manual_references_true() -> None:
    unmatched = pd.DataFrame(
        [{"survey_year": 2024, "survey_type": "RD", "reference": "SYN999", "instance": 1, "manual_outlier": True, "outlier_reason": "x"}]
    )
    with pytest.raises(ValueError, match="not present in imputed_responses"):
        apply_manual_outlier_adjustments(_imputed_df(), unmatched, strict_manual_references=True)


def test_unmatched_manual_row_ignored_when_strict_manual_references_false() -> None:
    unmatched = pd.DataFrame(
        [{"survey_year": 2024, "survey_type": "RD", "reference": "SYN999", "instance": 1, "manual_outlier": True, "outlier_reason": "x"}]
    )
    out = apply_manual_outlier_adjustments(_imputed_df(), unmatched, strict_manual_references=False)
    assert len(out) == len(_imputed_df())
    assert "SYN999" not in out["reference"].tolist()


def test_blank_outlier_reason_fails_clearly() -> None:
    bad_reason = pd.DataFrame(
        [{"survey_year": 2024, "survey_type": "RD", "reference": "SYN001", "instance": 1, "manual_outlier": True, "outlier_reason": "   "}]
    )
    with pytest.raises(ValueError, match="outlier_reason must be populated"):
        apply_manual_outlier_adjustments(_imputed_df(), bad_reason)


def test_invalid_manual_outlier_value_fails_clearly() -> None:
    bad_flag = pd.DataFrame(
        [{"survey_year": 2024, "survey_type": "RD", "reference": "SYN001", "instance": 1, "manual_outlier": "not_bool", "outlier_reason": "x"}]
    )
    with pytest.raises(ValueError, match="manual_outlier must be boolean-like"):
        apply_manual_outlier_adjustments(_imputed_df(), bad_flag)


def test_missing_imputed_response_grain_column_fails_clearly() -> None:
    imputed = _imputed_df().drop(columns=["reference"])
    with pytest.raises(ValueError, match="imputed_responses missing required grain columns"):
        apply_manual_outlier_adjustments(imputed, _manual_df())


def test_missing_manual_required_column_fails_clearly() -> None:
    manual = _manual_df().drop(columns=["manual_outlier"])
    with pytest.raises(ValueError, match="manual_outliers missing required columns"):
        apply_manual_outlier_adjustments(_imputed_df(), manual)


def test_input_dataframes_not_mutated() -> None:
    imputed = _imputed_df()
    manual = _manual_df()
    imputed_before = imputed.copy(deep=True)
    manual_before = manual.copy(deep=True)
    apply_manual_outlier_adjustments(imputed, manual)
    pd.testing.assert_frame_equal(imputed, imputed_before)
    pd.testing.assert_frame_equal(manual, manual_before)


def test_no_legacy_imports() -> None:
    importlib.import_module("src.randd_pipeline.domain.outliers.manual_adjustments")
    for forbidden in ["src.outlier_detection", "src.estimation", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

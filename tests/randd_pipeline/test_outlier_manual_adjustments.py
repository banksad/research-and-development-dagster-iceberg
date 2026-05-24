from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.domain.outliers import apply_manual_outlier_adjustments
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def test_expected_fixture_output() -> None:
    imputed = load_scenario_csv("imputation_to_outlier_minimal", "input_imputed_responses.csv")
    manual = load_scenario_csv("imputation_to_outlier_minimal", "input_ops_manual_outliers.csv")
    expected = load_scenario_csv("imputation_to_outlier_minimal", "expected_outlier_adjusted_responses.csv")
    actual = apply_manual_outlier_adjustments(imputed, manual)
    assert_frame_equal_sorted(actual, expected, sort_by=["reference", "instance", "survey_type", "survey_year"])


def test_failures_and_defaults() -> None:
    imputed = load_scenario_csv("imputation_to_outlier_minimal", "input_imputed_responses.csv")
    out = apply_manual_outlier_adjustments(imputed, None)
    assert (out["outlier_source"] == ["default_none", "default_none", "auto", "auto"]).all()

    bad = pd.DataFrame([{"survey_year": 2024, "survey_type": "RD", "reference": "SYN999", "instance": 1, "manual_outlier": True, "outlier_reason": "x"}])
    with pytest.raises(ValueError):
        apply_manual_outlier_adjustments(imputed, bad, strict_manual_references=True)

    with pytest.raises(ValueError):
        apply_manual_outlier_adjustments(imputed, pd.concat([bad, bad], ignore_index=True), strict_manual_references=False)


def test_invalid_manual_and_no_legacy_imports() -> None:
    imputed = load_scenario_csv("imputation_to_outlier_minimal", "input_imputed_responses.csv")
    bad_reason = pd.DataFrame([{"survey_year": 2024, "survey_type": "RD", "reference": "SYN001", "instance": 1, "manual_outlier": True, "outlier_reason": "   "}])
    with pytest.raises(ValueError):
        apply_manual_outlier_adjustments(imputed, bad_reason)

    bad_flag = bad_reason.copy()
    bad_flag["manual_outlier"] = "not_bool"
    with pytest.raises(ValueError):
        apply_manual_outlier_adjustments(imputed, bad_flag)

    importlib.import_module("src.randd_pipeline.domain.outliers.manual_adjustments")
    for forbidden in ["src.outlier_detection", "src.estimation", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.domain.estimation import calculate_minimal_estimation_weights
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def test_calculate_minimal_estimation_weights_matches_expected_fixture() -> None:
    input_df = load_scenario_csv("outlier_to_estimation_minimal", "input_outlier_adjusted_responses.csv")
    expected = load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv")
    actual = calculate_minimal_estimation_weights(input_df)
    assert_frame_equal_sorted(actual, expected, ["reference", "instance", "survey_type", "survey_year"])


def test_missing_required_columns_fail_clearly() -> None:
    df = load_scenario_csv("outlier_to_estimation_minimal", "input_outlier_adjusted_responses.csv").drop(columns=["uni_count"])
    with pytest.raises(ValueError, match="Missing required columns"):
        calculate_minimal_estimation_weights(df)


def test_invalid_numeric_values_fail_clearly() -> None:
    df = load_scenario_csv("outlier_to_estimation_minimal", "input_outlier_adjusted_responses.csv")
    df.loc[df.index[0], "employment"] = "bad"
    with pytest.raises(ValueError, match="numeric"):
        calculate_minimal_estimation_weights(df)


def test_outlier_and_non_selected_rows_default_to_one_weights() -> None:
    df = load_scenario_csv("outlier_to_estimation_minimal", "input_outlier_adjusted_responses.csv")
    out = calculate_minimal_estimation_weights(df)
    assert (out.loc[out["outlier"], ["a_weight", "g_weight"]] == 1.0).all().all()
    assert (out.loc[out["selectiontype"] != "P", ["a_weight", "g_weight"]] == 1.0).all().all()


def test_row_count_grain_and_input_immutability_preserved() -> None:
    df = load_scenario_csv("outlier_to_estimation_minimal", "input_outlier_adjusted_responses.csv")
    original = df.copy(deep=True)
    out = calculate_minimal_estimation_weights(df)
    pd.testing.assert_frame_equal(df, original)
    assert len(out) == len(df)
    for col in ["reference", "instance", "survey", "period"]:
        assert col in out.columns


def test_zero_denominator_and_no_eligible_rows_default_to_one_weights() -> None:
    df = load_scenario_csv("outlier_to_estimation_minimal", "input_outlier_adjusted_responses.csv")
    zero_denom = df.copy()
    zero_denom["uni_count"] = 0
    out_zero = calculate_minimal_estimation_weights(zero_denom)
    assert (out_zero[["a_weight", "g_weight"]] == 1.0).all().all()

    no_eligible = df.copy()
    no_eligible["selectiontype"] = "X"
    out_no_eligible = calculate_minimal_estimation_weights(no_eligible)
    assert (out_no_eligible[["a_weight", "g_weight"]] == 1.0).all().all()


def test_no_legacy_imports() -> None:
    importlib.import_module("src.randd_pipeline.domain.estimation.weights")
    for forbidden in ["src.estimation", "src.outlier_detection", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

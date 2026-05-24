from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.domain.site_apportionment import apply_explicit_site_apportionment
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def _inputs():
    return (
        load_scenario_csv(
            "estimation_to_site_apportionment_minimal",
            "input_estimated_responses.csv",
        ),
        load_scenario_csv(
            "estimation_to_site_apportionment_minimal",
            "input_site_apportionment_factors.csv",
        ),
    )


def test_apply_explicit_site_apportionment_matches_expected_fixture_output():
    est, fac = _inputs()
    expected = load_scenario_csv(
        "estimation_to_site_apportionment_minimal",
        "expected_site_apportioned_responses.csv",
    )

    out = apply_explicit_site_apportionment(est, fac, value_columns=["211"])

    assert_frame_equal_sorted(out, expected, ["reference", "site_id"])


def test_apply_explicit_site_apportionment_expands_syn001_to_multiple_sites():
    est, fac = _inputs()
    out = apply_explicit_site_apportionment(est, fac, value_columns=["211"])
    assert out.loc[out["reference"] == "SYN001", "site_id"].nunique() > 1


def test_apply_explicit_site_apportionment_sets_syn002_single_site_proportion_to_one():
    est, fac = _inputs()
    out = apply_explicit_site_apportionment(est, fac, value_columns=["211"])
    assert float(out.loc[out["reference"] == "SYN002", "site_proportion"].iloc[0]) == 1.0


def test_apply_explicit_site_apportionment_does_not_mutate_estimated_input():
    est, fac = _inputs()
    est_original = est.copy(deep=True)
    apply_explicit_site_apportionment(est, fac, value_columns=["211"])
    pd.testing.assert_frame_equal(est, est_original)


def test_apply_explicit_site_apportionment_does_not_mutate_factor_input():
    est, fac = _inputs()
    fac_original = fac.copy(deep=True)
    apply_explicit_site_apportionment(est, fac, value_columns=["211"])
    pd.testing.assert_frame_equal(fac, fac_original)


@pytest.mark.parametrize("missing_col", ["reference", "instance", "survey_type", "survey_year"])
def test_apply_explicit_site_apportionment_fails_for_missing_estimated_response_grain_column(missing_col: str):
    est, fac = _inputs()
    with pytest.raises(ValueError, match="estimated_responses missing required columns"):
        apply_explicit_site_apportionment(est.drop(columns=[missing_col]), fac, value_columns=["211"])


@pytest.mark.parametrize("missing_col", ["reference", "instance", "survey_type", "survey_year", "site_id", "site_proportion"])
def test_apply_explicit_site_apportionment_fails_for_missing_factor_required_column(missing_col: str):
    est, fac = _inputs()
    with pytest.raises(ValueError, match="site_factors missing required columns"):
        apply_explicit_site_apportionment(est, fac.drop(columns=[missing_col]), value_columns=["211"])


def test_apply_explicit_site_apportionment_fails_for_missing_requested_value_column():
    est, fac = _inputs()
    with pytest.raises(ValueError, match="Requested value column missing"):
        apply_explicit_site_apportionment(est, fac, value_columns=["missing"])


def test_apply_explicit_site_apportionment_fails_for_empty_value_columns():
    est, fac = _inputs()
    with pytest.raises(ValueError, match="value_columns must be non-empty"):
        apply_explicit_site_apportionment(est, fac, value_columns=[])


def test_apply_explicit_site_apportionment_fails_for_blank_output_suffix():
    est, fac = _inputs()
    with pytest.raises(ValueError, match="output_suffix must be non-blank"):
        apply_explicit_site_apportionment(est, fac, value_columns=["211"], output_suffix="  ")


@pytest.mark.parametrize("tolerance", [0, -0.01])
def test_apply_explicit_site_apportionment_fails_for_non_positive_factor_sum_tolerance(tolerance: float):
    est, fac = _inputs()
    with pytest.raises(ValueError, match="factor_sum_tolerance must be positive"):
        apply_explicit_site_apportionment(est, fac, value_columns=["211"], factor_sum_tolerance=tolerance)


def test_apply_explicit_site_apportionment_fails_for_non_numeric_value_column():
    est, fac = _inputs()
    est_bad = est.copy()
    est_bad.loc[0, "211"] = "not_numeric"
    with pytest.raises(ValueError, match="Value column must be numeric"):
        apply_explicit_site_apportionment(est_bad, fac, value_columns=["211"])


def test_apply_explicit_site_apportionment_fails_for_non_numeric_site_proportion():
    est, fac = _inputs()
    fac_bad = fac.copy()
    fac_bad.loc[0, "site_proportion"] = "not_numeric"
    with pytest.raises(ValueError, match="site_proportion must be numeric"):
        apply_explicit_site_apportionment(est, fac_bad, value_columns=["211"])


def test_apply_explicit_site_apportionment_fails_for_negative_site_proportion():
    est, fac = _inputs()
    fac_bad = fac.copy()
    fac_bad.loc[0, "site_proportion"] = -0.1
    with pytest.raises(ValueError, match="site_proportion must be non-negative"):
        apply_explicit_site_apportionment(est, fac_bad, value_columns=["211"])


def test_apply_explicit_site_apportionment_fails_for_duplicate_factor_response_site_grain():
    est, fac = _inputs()
    fac_bad = pd.concat([fac, fac.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicate factor rows"):
        apply_explicit_site_apportionment(est, fac_bad, value_columns=["211"])


def test_apply_explicit_site_apportionment_fails_when_factors_reference_missing_estimated_responses():
    est, fac = _inputs()
    fac_bad = fac.copy()
    fac_bad.loc[0, "reference"] = "MISSING_REF"
    with pytest.raises(ValueError, match="missing from estimated_responses"):
        apply_explicit_site_apportionment(est, fac_bad, value_columns=["211"])


def test_apply_explicit_site_apportionment_fails_for_missing_factor_coverage_when_strict():
    est, fac = _inputs()
    fac_bad = fac[fac["reference"] != "SYN002"]
    with pytest.raises(ValueError, match="Estimated responses missing site factors"):
        apply_explicit_site_apportionment(est, fac_bad, value_columns=["211"], strict_factor_coverage=True)


def test_apply_explicit_site_apportionment_drops_missing_factor_coverage_when_non_strict():
    est, fac = _inputs()
    fac_bad = fac[fac["reference"] != "SYN002"]
    out = apply_explicit_site_apportionment(est, fac_bad, value_columns=["211"], strict_factor_coverage=False)
    assert "SYN002" not in set(out["reference"])


def test_apply_explicit_site_apportionment_fails_when_factor_proportions_do_not_sum_to_one():
    est, fac = _inputs()
    fac_bad = fac.copy()
    fac_bad.loc[fac_bad["reference"] == "SYN001", "site_proportion"] = [0.2, 0.2]
    with pytest.raises(ValueError, match="sum to 1.0"):
        apply_explicit_site_apportionment(est, fac_bad, value_columns=["211"])


def test_no_legacy_imports():
    importlib.import_module("src.randd_pipeline.domain.site_apportionment.explicit_factors")
    for forbidden in [
        "src.site_apportionment",
        "src.estimation",
        "src.outlier_detection",
        "src.imputation",
        "src.mapping",
        "src.pipeline",
        "src.staging",
        "freezing",
        "construction",
    ]:
        assert forbidden not in importlib.sys.modules

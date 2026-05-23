from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.domain.imputation import apply_simple_tmi_imputation
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv

SCENARIO = "mapping_to_imputation_minimal"


def test_simple_tmi_matches_expected_fixture() -> None:
    mapped = load_scenario_csv(SCENARIO, "mapped_responses.csv")
    expected = load_scenario_csv(SCENARIO, "expected_imputed_responses.csv")
    actual = apply_simple_tmi_imputation(mapped, target_column="601", imputation_class_column="imp_class", status_column="status", clear_statuses={"clear", "responding"}, impute_statuses={"impute"}, output_column="601_imputed")
    assert_frame_equal_sorted(actual, expected, sort_by=["reference", "instance", "survey_type", "survey_year"])


def test_missing_columns_fails_clearly() -> None:
    mapped = load_scenario_csv(SCENARIO, "mapped_responses.csv").drop(columns=["imp_class"])
    with pytest.raises(ValueError, match="Missing required columns"):
        apply_simple_tmi_imputation(mapped, target_column="601", imputation_class_column="imp_class", status_column="status", clear_statuses={"clear"}, impute_statuses={"impute"})


def test_non_numeric_target_fails_clearly() -> None:
    mapped = load_scenario_csv(SCENARIO, "mapped_responses.csv")
    mapped.loc[mapped["status"].eq("clear"), "601"] = "abc"
    with pytest.raises(ValueError, match="must be numeric"):
        apply_simple_tmi_imputation(mapped, target_column="601", imputation_class_column="imp_class", status_column="status", clear_statuses={"clear"}, impute_statuses={"impute"})


def test_input_not_mutated() -> None:
    mapped = load_scenario_csv(SCENARIO, "mapped_responses.csv")
    before = mapped.copy(deep=True)
    _ = apply_simple_tmi_imputation(mapped, target_column="601", imputation_class_column="imp_class", status_column="status", clear_statuses={"clear", "responding"}, impute_statuses={"impute"})
    pd.testing.assert_frame_equal(mapped, before)


def test_no_legacy_imports() -> None:
    importlib.import_module("src.randd_pipeline.domain.imputation.simple_tmi")
    banned_prefixes = ["src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]
    assert not any(any(name == pref or name.startswith(pref + ".") for pref in banned_prefixes) for name in importlib.sys.modules)

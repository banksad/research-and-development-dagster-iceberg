from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.domain.outputs import build_curated_rnd_statistics
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def test_expected_fixture_output():
    inp = load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv")
    exp = load_scenario_csv("site_apportionment_to_curated_output_minimal", "expected_curated_rnd_statistics.csv")
    out = build_curated_rnd_statistics(inp)
    assert_frame_equal_sorted(out, exp, ["survey_year", "survey_type", "output_measure"])


def test_custom_mapping_and_validation_cases():
    inp = load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv")
    inp["999"] = [1, 2, 3]
    out = build_curated_rnd_statistics(inp, measure_columns={"211_apportioned": "total_211_apportioned", "999": "total_999"})
    assert set(out["output_measure"]) == {"total_211_apportioned", "total_999"}
    with pytest.raises(ValueError, match="non-empty"):
        build_curated_rnd_statistics(inp.iloc[0:0])
    with pytest.raises(ValueError, match="measure_columns must be non-empty"):
        build_curated_rnd_statistics(inp, measure_columns={})
    with pytest.raises(ValueError, match="blank source"):
        build_curated_rnd_statistics(inp, measure_columns={" ": "x"})
    with pytest.raises(ValueError, match="blank output"):
        build_curated_rnd_statistics(inp, measure_columns={"211_apportioned": " "})
    with pytest.raises(ValueError, match="group column"):
        build_curated_rnd_statistics(inp.drop(columns=["survey_type"]))
    with pytest.raises(ValueError, match="measure column"):
        build_curated_rnd_statistics(inp.drop(columns=["211_apportioned"]))
    bad = inp.copy(); bad.loc[0, "211_apportioned"] = "x"
    with pytest.raises(ValueError, match="non-numeric"):
        build_curated_rnd_statistics(bad)


def test_input_not_mutated_and_no_legacy_imports():
    inp = load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv")
    before = inp.copy(deep=True)
    _ = build_curated_rnd_statistics(inp)
    assert inp.equals(before)

    importlib.import_module("src.randd_pipeline.domain.outputs.curated_statistics")
    for forbidden in ["src.outputs", "src.site_apportionment", "src.estimation", "src.outlier_detection", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

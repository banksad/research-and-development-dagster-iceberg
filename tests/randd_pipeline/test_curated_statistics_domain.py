from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.domain.outputs import build_curated_rnd_statistics
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def _input_df():
    return load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv")


def test_expected_fixture_output():
    out = build_curated_rnd_statistics(_input_df())
    exp = load_scenario_csv("site_apportionment_to_curated_output_minimal", "expected_curated_rnd_statistics.csv")
    assert_frame_equal_sorted(out, exp, ["survey_year", "survey_type", "output_measure"])


def test_multiple_site_rows_aggregate_into_one_output_row():
    inp = _input_df()
    out = build_curated_rnd_statistics(inp)
    row = out[(out["survey_year"] == 2024) & (out["survey_type"] == "BERD") & (out["output_measure"] == "total_211_apportioned")]
    assert len(row) == 1
    assert float(row.iloc[0]["output_value"]) == pytest.approx(200.0)


def test_survey_groups_remain_separate():
    out = build_curated_rnd_statistics(_input_df())
    assert len(out[["survey_year", "survey_type"]].drop_duplicates()) == 2


def test_default_measure_mapping_works():
    out = build_curated_rnd_statistics(_input_df())
    assert set(out["output_measure"]) == {"total_211_apportioned"}


def test_custom_measure_mapping_with_second_numeric_column_works():
    inp = _input_df(); inp["999"] = [1.0, 2.0, 3.0]
    out = build_curated_rnd_statistics(inp, measure_columns={"211_apportioned": "total_211_apportioned", "999": "total_999"})
    assert set(out["output_measure"]) == {"total_211_apportioned", "total_999"}


def test_provenance_fields_passed_through_when_supplied():
    out = build_curated_rnd_statistics(_input_df(), source_snapshot_id="snap-1", pipeline_run_id="run-1")
    assert set(out["source_snapshot_id"]) == {"snap-1"}
    assert set(out["pipeline_run_id"]) == {"run-1"}


def test_input_dataframe_not_mutated():
    inp = _input_df(); before = inp.copy(deep=True)
    _ = build_curated_rnd_statistics(inp)
    assert inp.equals(before)


def test_empty_input_fails_clearly():
    with pytest.raises(ValueError, match="non-empty"):
        build_curated_rnd_statistics(_input_df().iloc[0:0])


def test_empty_measure_mapping_fails_clearly():
    with pytest.raises(ValueError, match="measure_columns must be non-empty"):
        build_curated_rnd_statistics(_input_df(), measure_columns={})


def test_blank_source_measure_column_fails_clearly():
    with pytest.raises(ValueError, match="blank source"):
        build_curated_rnd_statistics(_input_df(), measure_columns={" ": "x"})


def test_blank_output_measure_name_fails_clearly():
    with pytest.raises(ValueError, match="blank output"):
        build_curated_rnd_statistics(_input_df(), measure_columns={"211_apportioned": " "})


def test_missing_group_column_fails_clearly():
    with pytest.raises(ValueError, match="group column"):
        build_curated_rnd_statistics(_input_df().drop(columns=["survey_type"]))


def test_missing_measure_source_column_fails_clearly():
    with pytest.raises(ValueError, match="measure column"):
        build_curated_rnd_statistics(_input_df().drop(columns=["211_apportioned"]))


def test_non_numeric_measure_value_fails_clearly():
    bad = _input_df(); bad.loc[0, "211_apportioned"] = "x"
    with pytest.raises(ValueError, match="non-numeric"):
        build_curated_rnd_statistics(bad)


def test_null_measure_value_fails_clearly():
    bad = _input_df(); bad.loc[0, "211_apportioned"] = None
    with pytest.raises(ValueError, match="non-numeric or null"):
        build_curated_rnd_statistics(bad)


def test_no_legacy_imports():
    importlib.import_module("src.randd_pipeline.domain.outputs.curated_statistics")
    for forbidden in ["src.outputs", "src.site_apportionment", "src.estimation", "src.outlier_detection", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

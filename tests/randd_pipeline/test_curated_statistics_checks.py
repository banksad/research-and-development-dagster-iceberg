from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.curated_statistics_checks import (
    check_curated_rnd_statistics_non_empty,
    check_curated_rnd_statistics_output_measure_populated,
    check_curated_rnd_statistics_output_value_non_negative,
    check_curated_rnd_statistics_provenance_columns_present,
    check_curated_rnd_statistics_reconciles_to_site_input,
    check_curated_rnd_statistics_required_columns,
    check_curated_rnd_statistics_unique_grain,
)
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def _curated(): return load_scenario_csv("site_apportionment_to_curated_output_minimal", "expected_curated_rnd_statistics.csv")
def _site(): return load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv")

def test_required_columns_positive_and_negative_cases():
    assert check_curated_rnd_statistics_required_columns(_curated())[0]
    assert not check_curated_rnd_statistics_required_columns(_curated().drop(columns=["output_measure"]))[0]
    assert not check_curated_rnd_statistics_required_columns(_curated().drop(columns=["output_value"]))[0]
    assert not check_curated_rnd_statistics_required_columns(_curated().drop(columns=["survey_type"]))[0]
    assert not check_curated_rnd_statistics_required_columns(_curated().drop(columns=["pipeline_run_id"]))[0]

def test_non_empty_positive_and_negative_cases():
    assert check_curated_rnd_statistics_non_empty(_curated())[0]
    assert not check_curated_rnd_statistics_non_empty(_curated().iloc[0:0])[0]

def test_unique_grain_positive_and_negative_cases():
    assert check_curated_rnd_statistics_unique_grain(_curated())[0]
    assert not check_curated_rnd_statistics_unique_grain(pd.concat([_curated(), _curated().iloc[[0]]], ignore_index=True))[0]

def test_output_measure_populated_positive_and_negative_cases():
    assert check_curated_rnd_statistics_output_measure_populated(_curated())[0]
    bad = _curated(); bad.loc[0, "output_measure"] = None
    assert not check_curated_rnd_statistics_output_measure_populated(bad)[0]
    bad = _curated(); bad.loc[0, "output_measure"] = " "
    assert not check_curated_rnd_statistics_output_measure_populated(bad)[0]
    assert not check_curated_rnd_statistics_output_measure_populated(_curated().drop(columns=["output_measure"]))[0]

def test_output_value_non_negative_positive_and_negative_cases():
    assert check_curated_rnd_statistics_output_value_non_negative(_curated())[0]
    bad = _curated(); bad.loc[0, "output_value"] = None
    assert not check_curated_rnd_statistics_output_value_non_negative(bad)[0]
    bad = _curated(); bad.loc[0, "output_value"] = "x"
    assert not check_curated_rnd_statistics_output_value_non_negative(bad)[0]
    bad = _curated(); bad.loc[0, "output_value"] = -1
    assert not check_curated_rnd_statistics_output_value_non_negative(bad)[0]
    assert not check_curated_rnd_statistics_output_value_non_negative(_curated().drop(columns=["output_value"]))[0]

def test_provenance_columns_present_positive_and_negative_cases():
    assert check_curated_rnd_statistics_provenance_columns_present(_curated())[0]
    assert not check_curated_rnd_statistics_provenance_columns_present(_curated().drop(columns=["source_table_identifier"]))[0]
    assert not check_curated_rnd_statistics_provenance_columns_present(_curated().drop(columns=["source_snapshot_id"]))[0]
    assert not check_curated_rnd_statistics_provenance_columns_present(_curated().drop(columns=["pipeline_run_id"]))[0]

def test_reconciliation_positive_and_negative_cases():
    assert check_curated_rnd_statistics_reconciles_to_site_input(_curated(), _site())[0]
    assert not check_curated_rnd_statistics_reconciles_to_site_input(_curated()[_curated()["output_measure"] != "total_211_apportioned"], _site())[0]
    bad_curated = _curated(); bad_curated.loc[0, "output_value"] = "x"
    assert not check_curated_rnd_statistics_reconciles_to_site_input(bad_curated, _site())[0]
    bad_site = _site(); bad_site.loc[0, "211_apportioned"] = "x"
    assert not check_curated_rnd_statistics_reconciles_to_site_input(_curated(), bad_site)[0]
    assert not check_curated_rnd_statistics_reconciles_to_site_input(_curated(), _site().drop(columns=["survey_type"]))[0]
    assert not check_curated_rnd_statistics_reconciles_to_site_input(_curated().drop(columns=["output_measure"]), _site())[0]
    site = _site()
    site_missing_group = site[~((site["survey_year"] == 2025) & (site["survey_type"] == "GBERD"))]
    assert not check_curated_rnd_statistics_reconciles_to_site_input(_curated(), site_missing_group)[0]
    bad_totals = _curated(); bad_totals.loc[bad_totals["survey_year"] == 2024, "output_value"] = 0
    assert not check_curated_rnd_statistics_reconciles_to_site_input(bad_totals, _site())[0]

from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.curated_statistics_checks import *
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def test_checks_pass_and_fail_cases():
    curated = load_scenario_csv("site_apportionment_to_curated_output_minimal", "expected_curated_rnd_statistics.csv")
    site = load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv")
    for fn in [check_curated_rnd_statistics_required_columns, check_curated_rnd_statistics_non_empty, check_curated_rnd_statistics_unique_grain, check_curated_rnd_statistics_output_measure_populated, check_curated_rnd_statistics_output_value_non_negative, check_curated_rnd_statistics_provenance_columns_present]:
        assert fn(curated)[0]
    assert check_curated_rnd_statistics_reconciles_to_site_input(curated, site)[0]
    assert not check_curated_rnd_statistics_required_columns(curated.drop(columns=["output_value"]))[0]
    assert not check_curated_rnd_statistics_non_empty(curated.iloc[0:0])[0]
    assert not check_curated_rnd_statistics_unique_grain(pd.concat([curated, curated.iloc[[0]]], ignore_index=True))[0]
    bad = curated.copy(); bad.loc[0, "output_measure"] = " "
    assert not check_curated_rnd_statistics_output_measure_populated(bad)[0]
    bad = curated.copy(); bad.loc[0, "output_value"] = -1
    assert not check_curated_rnd_statistics_output_value_non_negative(bad)[0]
    assert not check_curated_rnd_statistics_provenance_columns_present(curated.drop(columns=["pipeline_run_id"]))[0]
    bad = curated.copy(); bad.loc[0, "output_value"] = 999
    assert not check_curated_rnd_statistics_reconciles_to_site_input(bad, site)[0]

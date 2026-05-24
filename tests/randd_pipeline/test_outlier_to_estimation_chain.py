from __future__ import annotations

import importlib

import pytest

dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def test_outlier_to_estimation_chain_with_checks(tmp_path) -> None:
    inp = load_scenario_csv("outlier_to_estimation_minimal", "input_outlier_adjusted_responses.csv")
    exp = load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv")
    res = TableStoreResource(catalog_name="outlier-estimation-chain", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = res.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, inp, overwrite=False)

    asset_mod = importlib.import_module("src.randd_pipeline.assets.estimation")
    check_mod = importlib.import_module("src.randd_pipeline.checks.estimation_asset_checks")
    defs = dagster.Definitions(
        assets=[asset_mod.estimated_responses],
        asset_checks=[check_mod.estimated_responses_table_exists, check_mod.estimated_responses_non_empty, check_mod.estimated_responses_required_columns, check_mod.estimated_responses_unique_grain, check_mod.estimated_responses_weights_populated, check_mod.estimated_responses_weights_positive],
        resources={"table_store": res},
    )
    assert defs.get_implicit_global_asset_job_def().execute_in_process().success
    assert defs.get_implicit_job_def_for_assets([asset_mod.estimated_responses.key]).execute_in_process().success
    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES)
    assert_frame_equal_sorted(actual, exp, ["reference", "instance", "survey_type", "survey_year"])

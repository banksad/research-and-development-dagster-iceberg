from __future__ import annotations

import importlib

import pytest

dagster = pytest.importorskip("dagster")
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def test_estimation_asset_checks_and_missing_table(tmp_path) -> None:
    mod = importlib.import_module("src.randd_pipeline.checks.estimation_asset_checks")
    assert mod._ASSET_KEY == dagster.AssetKey(["intermediate", "estimated_responses"])
    resource = TableStoreResource(catalog_name="est-check", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = dagster.Definitions(asset_checks=[mod.estimated_responses_non_empty], resources={"table_store": resource})
    res = defs.get_implicit_global_asset_job_def().execute_in_process()
    assert not res.success

    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES, load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv"), overwrite=False)
    checks = [mod.estimated_responses_table_exists, mod.estimated_responses_non_empty, mod.estimated_responses_required_columns, mod.estimated_responses_unique_grain, mod.estimated_responses_weights_populated, mod.estimated_responses_weights_positive]
    defs2 = dagster.Definitions(asset_checks=checks, resources={"table_store": resource})
    assert defs2.get_implicit_global_asset_job_def().execute_in_process().success

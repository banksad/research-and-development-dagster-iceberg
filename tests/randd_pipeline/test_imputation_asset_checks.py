from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def test_imputation_asset_checks_run_and_attach_asset_key(tmp_path) -> None:
    expected = load_scenario_csv("mapping_to_imputation_minimal", "expected_imputed_responses.csv")
    resource = TableStoreResource(catalog_name="imputation-checks", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES, expected, overwrite=False)

    mod = importlib.import_module("src.randd_pipeline.checks.imputation_asset_checks")
    checks = [getattr(mod, n) for n in [
        "imputed_responses_table_exists",
        "imputed_responses_non_empty",
        "imputed_responses_required_columns",
        "imputed_responses_unique_grain",
        "imputed_responses_imputation_marker_populated",
        "imputed_responses_no_illegal_missing_imputed_values",
    ]]
    defs = dagster.Definitions(asset_checks=checks, resources={"table_store": resource})
    result = defs.get_implicit_global_asset_job_def().execute_in_process()
    assert result.success


def test_imputation_asset_check_missing_table_fails(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="imputation-checks-missing", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    mod = importlib.import_module("src.randd_pipeline.checks.imputation_asset_checks")
    check = getattr(mod, "imputed_responses_table_exists")
    defs = dagster.Definitions(asset_checks=[check], resources={"table_store": resource})
    result = defs.get_implicit_global_asset_job_def().execute_in_process()
    assert not result.success

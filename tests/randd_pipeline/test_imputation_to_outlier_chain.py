from __future__ import annotations

import importlib

import pytest

dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def test_imputation_to_outlier_chain(tmp_path) -> None:
    imputed = load_scenario_csv("imputation_to_outlier_minimal", "input_imputed_responses.csv")
    manual = load_scenario_csv("imputation_to_outlier_minimal", "input_ops_manual_outliers.csv")
    expected = load_scenario_csv("imputation_to_outlier_minimal", "expected_outlier_adjusted_responses.csv")
    resource = TableStoreResource(catalog_name="outlier-chain", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES, imputed, overwrite=False)
    store.create_table_from_dataframe(refs.OPS_MANUAL_OUTLIERS, manual, overwrite=False)
    assets_mod = importlib.import_module("src.randd_pipeline.assets.outliers")
    checks_mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    defs = dagster.Definitions(assets=[getattr(assets_mod, "outlier_adjusted_responses")], asset_checks=[getattr(checks_mod, "outlier_adjusted_responses_required_columns")], resources={"table_store": resource})
    assert defs.get_implicit_global_asset_job_def().execute_in_process().success
    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
    assert_frame_equal_sorted(actual, expected, sort_by=["reference", "instance", "survey_type", "survey_year"])

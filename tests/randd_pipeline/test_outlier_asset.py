from __future__ import annotations

import importlib

import pytest

dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def test_outlier_adjusted_responses_materialises_expected_fixture_when_manual_table_exists(tmp_path) -> None:
    imputed = load_scenario_csv("imputation_to_outlier_minimal", "input_imputed_responses.csv")
    manual = load_scenario_csv("imputation_to_outlier_minimal", "input_ops_manual_outliers.csv")
    expected = load_scenario_csv("imputation_to_outlier_minimal", "expected_outlier_adjusted_responses.csv")
    resource = TableStoreResource(catalog_name="outlier-asset", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES, imputed, overwrite=False)
    store.create_table_from_dataframe(refs.OPS_MANUAL_OUTLIERS, manual, overwrite=False)

    mod = importlib.import_module("src.randd_pipeline.assets.outliers")
    defs = dagster.Definitions(assets=[mod.outlier_adjusted_responses], resources={"table_store": resource})
    assert defs.get_implicit_global_asset_job_def().execute_in_process().success
    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
    assert_frame_equal_sorted(actual, expected, sort_by=["reference", "instance", "survey_type", "survey_year"])


def test_outlier_adjusted_responses_materialises_default_auto_behaviour_when_manual_table_missing(tmp_path) -> None:
    imputed = load_scenario_csv("imputation_to_outlier_minimal", "input_imputed_responses.csv")
    resource = TableStoreResource(catalog_name="outlier-asset-no-manual", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES, imputed, overwrite=False)

    mod = importlib.import_module("src.randd_pipeline.assets.outliers")
    defs = dagster.Definitions(assets=[mod.outlier_adjusted_responses], resources={"table_store": resource})
    assert defs.get_implicit_global_asset_job_def().execute_in_process().success
    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
    assert "default_none" in actual["outlier_source"].tolist()
    assert "auto" in actual["outlier_source"].tolist()


def test_outlier_adjusted_responses_has_explicit_asset_key() -> None:
    mod = importlib.import_module("src.randd_pipeline.assets.outliers")
    assert mod.outlier_adjusted_responses.key == dagster.AssetKey(["intermediate", "outlier_adjusted_responses"])


def test_manual_outliers_asset_materialises_ops_manual_outliers_from_csv_with_run_config(tmp_path) -> None:
    mod = importlib.import_module("src.randd_pipeline.assets.outliers")
    csv_path = "tests/fixtures/synthetic/scenarios/imputation_to_outlier_minimal/input_ops_manual_outliers.csv"
    resource = TableStoreResource(catalog_name="manual-outlier-csv", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = dagster.Definitions(assets=[mod.manual_outliers], resources={"table_store": resource})
    result = defs.get_implicit_global_asset_job_def().execute_in_process(
        run_config={"ops": {"manual_outliers": {"config": {"manual_outliers_csv_path": csv_path}}}}
    )
    assert result.success
    store = resource.get_table_store()
    assert store.table_exists(refs.OPS_MANUAL_OUTLIERS)


def test_manual_outliers_input_config_rejects_blank_csv_path() -> None:
    mod = importlib.import_module("src.randd_pipeline.assets.outliers")
    with pytest.raises(Exception):
        mod.ManualOutliersInputConfig(manual_outliers_csv_path="   ")


def test_manual_outlier_adjustment_config_defaults() -> None:
    mod = importlib.import_module("src.randd_pipeline.assets.outliers")
    config = mod.ManualOutlierAdjustmentConfig()
    assert config.strict_manual_references is True
    assert config.default_outlier is False


def test_no_legacy_imports() -> None:
    importlib.import_module("src.randd_pipeline.assets.outliers")
    for forbidden in ["src.outlier_detection", "src.estimation", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

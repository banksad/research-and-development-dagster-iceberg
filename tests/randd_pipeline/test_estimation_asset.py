from __future__ import annotations

import importlib

import pytest

dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def test_estimated_responses_materialises_expected_fixture(tmp_path) -> None:
    in_df = load_scenario_csv("outlier_to_estimation_minimal", "input_outlier_adjusted_responses.csv")
    expected = load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv")
    resource = TableStoreResource(catalog_name="estimation-asset", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, in_df, overwrite=False)
    mod = importlib.import_module("src.randd_pipeline.assets.estimation")
    defs = dagster.Definitions(assets=[mod.estimated_responses], resources={"table_store": resource})
    assert defs.get_implicit_global_asset_job_def().execute_in_process().success
    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES)
    assert_frame_equal_sorted(actual, expected, ["reference", "instance", "survey_type", "survey_year"])


def test_estimation_asset_key_and_config_validation() -> None:
    mod = importlib.import_module("src.randd_pipeline.assets.estimation")
    assert mod.estimated_responses.key == dagster.AssetKey(["intermediate", "estimated_responses"])
    defaults = mod.MinimalEstimationWeightsConfig()
    assert defaults.selected_form_type == "0006"
    assert defaults.clear_statuses == ["Clear", "Clear - overridden"]
    with pytest.raises(Exception, match="non-blank"): mod.MinimalEstimationWeightsConfig(cell_column=" ")
    with pytest.raises(Exception, match="non-empty"): mod.MinimalEstimationWeightsConfig(clear_statuses=[])
    with pytest.raises(Exception, match="blank"): mod.MinimalEstimationWeightsConfig(clear_statuses=["Clear", " "])


def test_estimated_responses_run_config_works(tmp_path) -> None:
    in_df = load_scenario_csv("outlier_to_estimation_minimal", "input_outlier_adjusted_responses.csv")
    expected = load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv")
    resource = TableStoreResource(catalog_name="estimation-runconfig", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, in_df, overwrite=False)
    mod = importlib.import_module("src.randd_pipeline.assets.estimation")
    defs = dagster.Definitions(assets=[mod.estimated_responses], resources={"table_store": resource})
    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config={"ops": {"estimated_responses": {"config": {"clear_statuses": ["Clear", "Clear - overridden"]}}}})
    assert result.success
    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES)
    assert_frame_equal_sorted(actual, expected, ["reference", "instance", "survey_type", "survey_year"])


def test_no_legacy_imports() -> None:
    importlib.import_module("src.randd_pipeline.assets.estimation")
    for forbidden in ["src.estimation", "src.outlier_detection", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv


def test_imputed_asset_materialises_expected_fixture(tmp_path) -> None:
    mapped = load_scenario_csv("mapping_to_imputation_minimal", "mapped_responses.csv")
    expected = load_scenario_csv("mapping_to_imputation_minimal", "expected_imputed_responses.csv")
    resource = TableStoreResource(catalog_name="imputation-asset-smoke", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, mapped, overwrite=False)

    mod = importlib.import_module("src.randd_pipeline.assets.imputation")
    asset_fn = getattr(mod, "imputed_responses")
    defs = dagster.Definitions(assets=[asset_fn], resources={"table_store": resource})
    result = defs.get_implicit_global_asset_job_def().execute_in_process()
    assert result.success
    assert asset_fn.key == dagster.AssetKey(["intermediate", "imputed_responses"])
    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES)
    assert_frame_equal_sorted(actual, expected, sort_by=["reference", "instance", "survey_type", "survey_year"])
    assert "src.imputation" not in importlib.sys.modules


def test_simple_tmi_imputation_config_defaults() -> None:
    mod = importlib.import_module("src.randd_pipeline.assets.imputation")
    config = getattr(mod, "SimpleTmiImputationConfig")()

    assert config.target_column == "601"
    assert config.imputation_class_column == "imp_class"
    assert config.status_column == "status"
    assert config.clear_statuses == ["clear", "responding"]
    assert config.impute_statuses == ["impute", "non_response"]
    assert config.output_column == "601_imputed"
    assert config.marker_column == "imp_marker"


def test_simple_tmi_imputation_config_validation() -> None:
    mod = importlib.import_module("src.randd_pipeline.assets.imputation")
    config_cls = getattr(mod, "SimpleTmiImputationConfig")

    with pytest.raises(ValueError, match="Column names must be non-blank strings"):
        config_cls(target_column="   ")

    with pytest.raises(ValueError, match="Status lists must contain at least one value"):
        config_cls(clear_statuses=[])

    with pytest.raises(ValueError, match="Status values must be unique"):
        config_cls(impute_statuses=["impute", "impute"])

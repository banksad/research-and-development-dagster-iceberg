from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, scenario_path

SCENARIO_ID = "staging_to_cell_number_mapping_minimal"

dagster = pytest.importorskip("dagster")
MAPPED_RESPONSES_ASSET_KEY = dagster.AssetKey(["intermediate", "mapped_responses"])
ULTFOC_MAPPER_ASSET_KEY = dagster.AssetKey(["ref", "ultfoc_mapper"])
CELL_NUMBER_MAPPER_ASSET_KEY = dagster.AssetKey(["ref", "cell_number_mapper"])


def _mapping_run_config() -> dict:
    scenario = scenario_path(SCENARIO_ID)
    return {
        "ops": {
            "ultfoc_mapper": {"config": {"ultfoc_mapper_csv_path": str(scenario / "ultfoc_mapper.csv")}},
            "cell_number_mapper": {"config": {"cell_number_mapper_csv_path": str(scenario / "cell_number_mapper.csv")}}
        }
    }


def test_mapping_assets_use_explicit_layered_asset_keys() -> None:
    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    mapped_responses_asset = getattr(mapping_mod, "mapped_responses")
    ultfoc_mapper_asset = getattr(mapping_mod, "ultfoc_mapper")

    assert mapped_responses_asset.key == MAPPED_RESPONSES_ASSET_KEY
    assert ultfoc_mapper_asset.key == ULTFOC_MAPPER_ASSET_KEY


def test_mapped_responses_asset_materialises_fixture_to_local_table_store(tmp_path) -> None:
    scenario = scenario_path(SCENARIO_ID)
    staged_path = scenario / "staged_responses.csv"
    expected_path = scenario / "expected_cell_number_mapped_responses.csv"

    resource = TableStoreResource(
        catalog_name="mapping-asset-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    store = resource.get_table_store()
    staged = pd.read_csv(staged_path)
    store.create_table_from_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES, staged, overwrite=False)

    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    mapped_responses_asset = getattr(mapping_mod, "mapped_responses")
    ultfoc_mapper_asset = getattr(mapping_mod, "ultfoc_mapper")

    cell_number_mapper_asset = getattr(mapping_mod, "cell_number_mapper")

    defs = dagster.Definitions(assets=[ultfoc_mapper_asset, cell_number_mapper_asset, mapped_responses_asset], resources={"table_store": resource})
    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_mapping_run_config())
    assert result.success

    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
    expected = pd.read_csv(expected_path)
    assert_frame_equal_sorted(actual, expected, sort_by=["reference", "instance", "survey_type", "survey_year"])


def test_mapped_responses_asset_duplicate_materialisation_fails_when_table_exists(tmp_path) -> None:
    scenario = scenario_path(SCENARIO_ID)

    resource = TableStoreResource(
        catalog_name="mapping-asset-duplicate-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    store = resource.get_table_store()
    staged = pd.read_csv(scenario / "staged_responses.csv")
    store.create_table_from_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES, staged, overwrite=False)

    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    mapped_responses_asset = getattr(mapping_mod, "mapped_responses")
    ultfoc_mapper_asset = getattr(mapping_mod, "ultfoc_mapper")

    cell_number_mapper_asset = getattr(mapping_mod, "cell_number_mapper")

    defs = dagster.Definitions(assets=[ultfoc_mapper_asset, cell_number_mapper_asset, mapped_responses_asset], resources={"table_store": resource})
    run_config = _mapping_run_config()

    first_result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=run_config)
    assert first_result.success

    with pytest.raises(ValueError, match="already exists"):
        defs.get_implicit_global_asset_job_def().execute_in_process(run_config=run_config, raise_on_error=True)


def test_mapping_asset_smoke_does_not_import_legacy_modules(tmp_path) -> None:
    scenario = scenario_path(SCENARIO_ID)

    resource = TableStoreResource(
        catalog_name="mapping-asset-import-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    store = resource.get_table_store()
    staged = pd.read_csv(scenario / "staged_responses.csv")
    store.create_table_from_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES, staged, overwrite=False)

    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    mapped_responses_asset = getattr(mapping_mod, "mapped_responses")
    ultfoc_mapper_asset = getattr(mapping_mod, "ultfoc_mapper")

    cell_number_mapper_asset = getattr(mapping_mod, "cell_number_mapper")

    defs = dagster.Definitions(assets=[ultfoc_mapper_asset, cell_number_mapper_asset, mapped_responses_asset], resources={"table_store": resource})
    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_mapping_run_config())

    assert result.success
    assert not any(name == "src.mapping" or name.startswith("src.mapping.") for name in importlib.sys.modules)
    assert "src.pipeline" not in importlib.sys.modules
    assert not any(name == "src.staging" or name.startswith("src.staging.") for name in importlib.sys.modules)
    assert not any(name == "freezing" or name.startswith("freezing.") for name in importlib.sys.modules)
    assert not any(name == "construction" or name.startswith("construction.") for name in importlib.sys.modules)

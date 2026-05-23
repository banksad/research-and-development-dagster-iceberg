from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, scenario_path

SCENARIO_ID = "mapping_cell_number_minimal"
CELL_MAPPER_ASSET_KEY = dagster.AssetKey(["ref", "cell_number_mapper"])
CELL_MAPPED_ASSET_KEY = dagster.AssetKey(["intermediate", "cell_number_mapped_responses"])


def _run_config() -> dict:
    scenario = scenario_path(SCENARIO_ID)
    return {
        "ops": {
            "cell_number_mapper": {
                "config": {"cell_number_mapper_csv_path": str(scenario / "cell_number_mapper.csv")}
            }
        }
    }


def test_cell_number_mapping_assets_use_explicit_asset_keys() -> None:
    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    assert getattr(mapping_mod, "cell_number_mapper").key == CELL_MAPPER_ASSET_KEY
    assert getattr(mapping_mod, "cell_number_mapped_responses").key == CELL_MAPPED_ASSET_KEY


def test_cell_number_mapping_assets_materialise_expected_tables(tmp_path) -> None:
    scenario = scenario_path(SCENARIO_ID)
    resource = TableStoreResource("cell-mapping", "local_sql", str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, pd.read_csv(scenario / "mapped_responses.csv"))

    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    defs = dagster.Definitions(
        assets=[getattr(mapping_mod, "cell_number_mapper"), getattr(mapping_mod, "cell_number_mapped_responses")],
        resources={"table_store": resource},
    )
    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_run_config())
    assert result.success

    mapper_actual = store.read_table_as_dataframe(refs.REF_CELL_NUMBER_MAPPER)
    assert list(mapper_actual.columns) == ["cellnumber", "uni_count", "uni_employment"]

    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES)
    expected = pd.read_csv(scenario / "expected_cell_number_mapped_responses.csv")
    assert_frame_equal_sorted(actual, expected, ["reference", "instance", "survey_type", "survey_year"])


def test_cell_number_mapping_assets_duplicate_materialisation_fails(tmp_path) -> None:
    scenario = scenario_path(SCENARIO_ID)
    resource = TableStoreResource("cell-mapping-dupe", "local_sql", str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, pd.read_csv(scenario / "mapped_responses.csv"))

    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    defs = dagster.Definitions(
        assets=[getattr(mapping_mod, "cell_number_mapper"), getattr(mapping_mod, "cell_number_mapped_responses")],
        resources={"table_store": resource},
    )
    assert defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_run_config()).success

    with pytest.raises(ValueError, match="already exists"):
        defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_run_config(), raise_on_error=True)


def test_cell_number_mapping_asset_module_does_not_import_legacy_modules() -> None:
    importlib.import_module("src.randd_pipeline.assets.mapping")
    assert not any(name == "src.mapping" or name.startswith("src.mapping.") for name in importlib.sys.modules)
    assert "src.pipeline" not in importlib.sys.modules

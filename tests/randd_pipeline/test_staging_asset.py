from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, scenario_path

SCENARIO_ID = "staging_minimal_valid_responses"

dagster = pytest.importorskip("dagster")
STAGED_RESPONSES_ASSET_KEY = dagster.AssetKey(["intermediate", "staged_responses"])


def _staging_run_config() -> dict:
    scenario = scenario_path(SCENARIO_ID)
    return {
        "ops": {
            "staged_responses": {
                "config": {
                    "contributors_csv_path": str(scenario / "contributors.csv"),
                    "responses_long_csv_path": str(scenario / "responses_long.csv"),
                }
            }
        }
    }


def test_staged_responses_asset_uses_explicit_layered_asset_key() -> None:
    staging_mod = importlib.import_module("src.randd_pipeline.assets.staging")
    staged_responses_asset = getattr(staging_mod, "staged_responses")

    assert staged_responses_asset.key == STAGED_RESPONSES_ASSET_KEY


def test_staged_responses_asset_materialises_fixture_to_local_table_store(tmp_path) -> None:
    expected_path = scenario_path(SCENARIO_ID) / "expected_full_responses.csv"

    resource = TableStoreResource(
        catalog_name="staging-asset-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )

    staging_mod = importlib.import_module("src.randd_pipeline.assets.staging")
    staged_responses_asset = getattr(staging_mod, "staged_responses")

    defs = dagster.Definitions(assets=[staged_responses_asset], resources={"table_store": resource})
    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_staging_run_config())
    assert result.success

    store = resource.get_table_store()
    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES)
    expected = pd.read_csv(expected_path)

    assert_frame_equal_sorted(
        actual,
        expected,
        sort_by=["reference", "instance", "survey_type", "survey_year"],
    )


def test_staged_responses_asset_duplicate_materialisation_fails_when_table_exists(tmp_path) -> None:
    resource = TableStoreResource(
        catalog_name="staging-asset-duplicate-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )

    staging_mod = importlib.import_module("src.randd_pipeline.assets.staging")
    staged_responses_asset = getattr(staging_mod, "staged_responses")

    defs = dagster.Definitions(assets=[staged_responses_asset], resources={"table_store": resource})
    run_config = _staging_run_config()

    first_result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=run_config)
    assert first_result.success

    with pytest.raises(ValueError, match="already exists"):
        defs.get_implicit_global_asset_job_def().execute_in_process(run_config=run_config, raise_on_error=True)


def test_staging_asset_smoke_does_not_import_legacy_modules(tmp_path) -> None:
    resource = TableStoreResource(
        catalog_name="staging-asset-import-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )

    staging_mod = importlib.import_module("src.randd_pipeline.assets.staging")
    staged_responses_asset = getattr(staging_mod, "staged_responses")

    defs = dagster.Definitions(assets=[staged_responses_asset], resources={"table_store": resource})
    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_staging_run_config())

    assert result.success
    assert "src.pipeline" not in importlib.sys.modules
    assert not any(name == "src.staging" or name.startswith("src.staging.") for name in importlib.sys.modules)
    assert not any(name == "freezing" or name.startswith("freezing.") for name in importlib.sys.modules)
    assert not any(name == "construction" or name.startswith("construction.") for name in importlib.sys.modules)

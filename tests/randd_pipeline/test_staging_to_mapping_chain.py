from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, scenario_path

SCENARIO_ID = "staging_to_mapping_minimal"
STAGED_RESPONSES_ASSET_KEY = dagster.AssetKey(["intermediate", "staged_responses"])
MAPPED_RESPONSES_ASSET_KEY = dagster.AssetKey(["intermediate", "mapped_responses"])


def _run_config() -> dict:
    scenario = scenario_path(SCENARIO_ID)
    return {
        "ops": {
            "staged_responses": {
                "config": {
                    "contributors_csv_path": str(scenario / "contributors.csv"),
                    "responses_long_csv_path": str(scenario / "responses_long.csv"),
                }
            },
            "ultfoc_mapper": {"config": {"ultfoc_mapper_csv_path": str(scenario / "ultfoc_mapper.csv")}},
        }
    }


def _build_defs(resource: TableStoreResource):
    staging_mod = importlib.import_module("src.randd_pipeline.assets.staging")
    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    staging_checks_mod = importlib.import_module("src.randd_pipeline.checks.staging_asset_checks")
    mapping_checks_mod = importlib.import_module("src.randd_pipeline.checks.mapping_asset_checks")

    return dagster.Definitions(
        assets=[
            getattr(staging_mod, "staged_responses"),
            getattr(mapping_mod, "ultfoc_mapper"),
            getattr(mapping_mod, "mapped_responses"),
        ],
        asset_checks=[
            getattr(staging_checks_mod, "staged_responses_table_exists"),
            getattr(staging_checks_mod, "staged_responses_non_empty"),
            getattr(staging_checks_mod, "staged_responses_required_columns"),
            getattr(staging_checks_mod, "staged_responses_unique_grain"),
            getattr(mapping_checks_mod, "mapped_responses_table_exists"),
            getattr(mapping_checks_mod, "mapped_responses_non_empty"),
            getattr(mapping_checks_mod, "mapped_responses_required_columns"),
            getattr(mapping_checks_mod, "mapped_responses_unique_grain"),
            getattr(mapping_checks_mod, "mapped_responses_ultfoc_present"),
        ],
        resources={"table_store": resource},
    )


def test_staging_to_mapping_chain_materialises_tables_and_hits_expected_mapped_transition_check_state(tmp_path) -> None:
    scenario = scenario_path(SCENARIO_ID)
    resource = TableStoreResource(
        catalog_name="staging-to-mapping-chain-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    defs = _build_defs(resource)

    materialise_result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_run_config())
    assert materialise_result.success

    store = resource.get_table_store()
    actual_staged = store.read_table_as_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES)
    expected_staged = pd.read_csv(scenario / "expected_staged_responses.csv")
    assert_frame_equal_sorted(actual_staged, expected_staged, sort_by=["reference", "instance", "survey_type", "survey_year"])

    actual_mapped = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
    expected_mapped = pd.read_csv(scenario / "expected_mapped_responses.csv")
    assert_frame_equal_sorted(actual_mapped, expected_mapped, sort_by=["reference", "instance", "survey_type", "survey_year"])

    staged_checks_result = defs.get_asset_checks_def(STAGED_RESPONSES_ASSET_KEY).execute_in_process()
    assert staged_checks_result.success

    mapped_checks_result = defs.get_asset_checks_def(MAPPED_RESPONSES_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not mapped_checks_result.success
    mapped_messages = [e.event_specific_data.description for e in mapped_checks_result.get_asset_check_evaluations()]
    # Intentional transition: canonical mapped_responses required-columns reflects v1 target
    # while staging_to_mapping_minimal runtime output remains foreign-ownership-only for now.
    assert any("Missing required columns" in m for m in mapped_messages)
    assert not any("contains duplicate row(s)" in m for m in mapped_messages)
    assert not any("null/blank" in m for m in mapped_messages)

    assert "src.pipeline" not in importlib.sys.modules
    assert not any(name == "src.staging" or name.startswith("src.staging.") for name in importlib.sys.modules)
    assert not any(name == "src.mapping" or name.startswith("src.mapping.") for name in importlib.sys.modules)
    assert not any(name == "freezing" or name.startswith("freezing.") for name in importlib.sys.modules)
    assert not any(name == "construction" or name.startswith("construction.") for name in importlib.sys.modules)

from __future__ import annotations

import importlib

import pytest

from src.randd_pipeline.assets.inputs import load_raw_full_responses_from_csv
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import scenario_path

SCENARIO_ID = "basic_responses"

dagster = pytest.importorskip("dagster")


def _build_defs(resource: TableStoreResource):
    inputs_mod = importlib.import_module("src.randd_pipeline.assets.inputs")
    checks_mod = importlib.import_module("src.randd_pipeline.checks.raw_input_checks")
    raw_full_responses_asset = getattr(inputs_mod, "raw_full_responses")

    return dagster.Definitions(
        assets=[raw_full_responses_asset],
        asset_checks=[
            getattr(checks_mod, "raw_full_responses_table_exists"),
            getattr(checks_mod, "raw_full_responses_non_empty"),
            getattr(checks_mod, "raw_full_responses_required_columns"),
        ],
        resources={"table_store": resource},
    )


def test_raw_full_responses_asset_checks_pass_after_materialisation(tmp_path) -> None:
    raw_path = scenario_path(SCENARIO_ID) / "raw_full_responses.csv"
    resource = TableStoreResource(
        catalog_name="raw-input-check-pass",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    defs = _build_defs(resource)

    result = defs.get_implicit_global_asset_job_def().execute_in_process(
        run_config={"ops": {"raw_full_responses": {"config": {"csv_path": str(raw_path)}}}}
    )
    assert result.success

    check_result = defs.get_asset_checks_def("raw_full_responses").execute_in_process()
    assert check_result.success


def test_raw_full_responses_asset_checks_fail_clearly_when_table_is_missing(tmp_path) -> None:
    resource = TableStoreResource(
        catalog_name="raw-input-check-missing",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    defs = _build_defs(resource)

    check_result = defs.get_asset_checks_def("raw_full_responses").execute_in_process(raise_on_error=False)

    assert not check_result.success
    messages = [event.event_specific_data.description for event in check_result.get_asset_check_evaluations()]
    assert any("does not exist" in message for message in messages)


def test_raw_full_responses_required_columns_check_fails_clearly_when_columns_missing(tmp_path) -> None:
    resource = TableStoreResource(
        catalog_name="raw-input-check-columns",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    defs = _build_defs(resource)

    store = resource.get_table_store()
    store.create_table_from_dataframe(
        refs.RAW_FULL_RESPONSES,
        load_raw_full_responses_from_csv(scenario_path(SCENARIO_ID) / "raw_full_responses.csv").drop(columns=["instance"]),
    )

    check_result = defs.get_asset_checks_def("raw_full_responses").execute_in_process(raise_on_error=False)

    assert not check_result.success
    messages = [event.event_specific_data.description for event in check_result.get_asset_check_evaluations()]
    assert any("Missing required columns: instance" in message for message in messages)


def test_raw_input_checks_do_not_import_legacy_pipeline_or_staging_modules(tmp_path) -> None:
    raw_path = scenario_path(SCENARIO_ID) / "raw_full_responses.csv"
    resource = TableStoreResource(
        catalog_name="raw-input-check-import-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    defs = _build_defs(resource)

    result = defs.get_implicit_global_asset_job_def().execute_in_process(
        run_config={"ops": {"raw_full_responses": {"config": {"csv_path": str(raw_path)}}}}
    )
    assert result.success

    check_result = defs.get_asset_checks_def("raw_full_responses").execute_in_process()
    assert check_result.success
    assert "src.pipeline" not in importlib.sys.modules
    assert not any(name == "src.staging" or name.startswith("src.staging.") for name in importlib.sys.modules)

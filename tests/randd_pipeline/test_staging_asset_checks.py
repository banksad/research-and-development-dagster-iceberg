from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import scenario_path

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


def _build_defs(resource: TableStoreResource):
    staging_mod = importlib.import_module("src.randd_pipeline.assets.staging")
    checks_mod = importlib.import_module("src.randd_pipeline.checks.staging_asset_checks")

    return dagster.Definitions(
        assets=[getattr(staging_mod, "staged_responses")],
        asset_checks=[
            getattr(checks_mod, "staged_responses_table_exists"),
            getattr(checks_mod, "staged_responses_non_empty"),
            getattr(checks_mod, "staged_responses_required_columns"),
            getattr(checks_mod, "staged_responses_unique_grain"),
        ],
        resources={"table_store": resource},
    )


def _check_messages(check_result) -> list[str]:
    return [event.event_specific_data.description for event in check_result.get_asset_check_evaluations()]


def test_staged_asset_checks_pass_after_materialisation(tmp_path) -> None:
    resource = TableStoreResource("staging-check-pass", "local_sql", str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_staging_run_config())
    assert result.success

    check_result = defs.get_asset_checks_def(STAGED_RESPONSES_ASSET_KEY).execute_in_process()
    assert check_result.success


def test_staged_asset_checks_fail_clearly_when_table_missing(tmp_path) -> None:
    resource = TableStoreResource("staging-check-missing", "local_sql", str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    check_result = defs.get_asset_checks_def(STAGED_RESPONSES_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("does not exist" in message for message in _check_messages(check_result))


def test_staged_required_columns_check_fails_clearly_when_columns_missing(tmp_path) -> None:
    resource = TableStoreResource("staging-check-columns", "local_sql", str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    store = resource.get_table_store()
    store.create_table_from_dataframe(
        refs.INTERMEDIATE_STAGED_RESPONSES,
        pd.DataFrame({"reference": ["A"], "instance": [1], "survey_type": ["BERD"]}),
    )

    check_result = defs.get_asset_checks_def(STAGED_RESPONSES_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("Missing required columns: survey_year" in message for message in _check_messages(check_result))


def test_staged_unique_grain_check_fails_clearly_when_duplicates_present(tmp_path) -> None:
    resource = TableStoreResource("staging-check-dupes", "local_sql", str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    store = resource.get_table_store()
    duplicate_df = pd.DataFrame(
        {
            "reference": ["A", "A"],
            "instance": [1, 1],
            "survey_type": ["BERD", "BERD"],
            "survey_year": [2024, 2024],
        }
    )
    store.create_table_from_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES, duplicate_df)

    check_result = defs.get_asset_checks_def(STAGED_RESPONSES_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("duplicate row(s)" in message for message in _check_messages(check_result))


def test_staged_checks_attach_to_explicit_asset_key() -> None:
    checks_mod = importlib.import_module("src.randd_pipeline.checks.staging_asset_checks")
    assert getattr(checks_mod, "staged_responses_table_exists").asset_key == STAGED_RESPONSES_ASSET_KEY


def test_staged_checks_do_not_import_legacy_modules(tmp_path) -> None:
    resource = TableStoreResource("staging-check-import-smoke", "local_sql", str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_staging_run_config())
    assert result.success

    check_result = defs.get_asset_checks_def(STAGED_RESPONSES_ASSET_KEY).execute_in_process()
    assert check_result.success
    assert "src.pipeline" not in importlib.sys.modules
    assert not any(name == "src.staging" or name.startswith("src.staging.") for name in importlib.sys.modules)
    assert not any(name == "freezing" or name.startswith("freezing.") for name in importlib.sys.modules)
    assert not any(name == "construction" or name.startswith("construction.") for name in importlib.sys.modules)

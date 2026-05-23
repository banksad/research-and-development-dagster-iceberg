from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import scenario_path

SCENARIO_ID = "mapping_foreign_ownership_minimal"
ULTFOC_MAPPER_ASSET_KEY = dagster.AssetKey(["ref", "ultfoc_mapper"])


def _run_config() -> dict:
    scenario = scenario_path(SCENARIO_ID)
    return {"ops": {"ultfoc_mapper": {"config": {"ultfoc_mapper_csv_path": str(scenario / "ultfoc_mapper.csv")}}}}


def _build_defs(resource: TableStoreResource):
    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    checks_mod = importlib.import_module("src.randd_pipeline.checks.ref_asset_checks")

    return dagster.Definitions(
        assets=[getattr(mapping_mod, "ultfoc_mapper")],
        asset_checks=[
            getattr(checks_mod, "ultfoc_mapper_table_exists"),
            getattr(checks_mod, "ultfoc_mapper_non_empty"),
            getattr(checks_mod, "ultfoc_mapper_required_columns"),
            getattr(checks_mod, "ultfoc_mapper_unique_ruref"),
        ],
        resources={"table_store": resource},
    )


def _check_messages(check_result) -> list[str]:
    return [event.event_specific_data.description for event in check_result.get_asset_check_evaluations()]


def test_ref_ultfoc_mapper_asset_checks_pass_after_materialisation(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="ref-check-pass", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_run_config())
    assert result.success

    check_result = defs.get_asset_checks_def(ULTFOC_MAPPER_ASSET_KEY).execute_in_process()
    assert check_result.success


def test_ref_ultfoc_mapper_asset_checks_fail_clearly_when_table_missing(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="ref-check-missing", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    check_result = defs.get_asset_checks_def(ULTFOC_MAPPER_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("does not exist" in message for message in _check_messages(check_result))


def test_ref_ultfoc_mapper_required_columns_check_fails_clearly_when_columns_missing(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="ref-check-columns", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    resource.get_table_store().create_table_from_dataframe(refs.REF_ULTFOC_MAPPER, pd.DataFrame({"ruref": ["A"]}))

    check_result = defs.get_asset_checks_def(ULTFOC_MAPPER_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("Missing required columns: ultfoc" in message for message in _check_messages(check_result))


def test_ref_ultfoc_mapper_unique_ruref_check_fails_clearly_when_duplicates_present(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="ref-check-dupes", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    duplicate_df = pd.DataFrame({"ruref": ["A", "A"], "ultfoc": ["US", "US"]})
    resource.get_table_store().create_table_from_dataframe(refs.REF_ULTFOC_MAPPER, duplicate_df)

    check_result = defs.get_asset_checks_def(ULTFOC_MAPPER_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("duplicate row(s)" in message for message in _check_messages(check_result))


def test_ref_ultfoc_mapper_checks_attach_to_explicit_asset_key() -> None:
    checks_mod = importlib.import_module("src.randd_pipeline.checks.ref_asset_checks")
    assert getattr(checks_mod, "ultfoc_mapper_table_exists").asset_key == ULTFOC_MAPPER_ASSET_KEY


def test_ref_ultfoc_mapper_checks_do_not_import_legacy_modules(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="ref-check-import-smoke", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_run_config())
    assert result.success

    check_result = defs.get_asset_checks_def(ULTFOC_MAPPER_ASSET_KEY).execute_in_process()
    assert check_result.success
    assert not any(name == "src.mapping" or name.startswith("src.mapping.") for name in importlib.sys.modules)
    assert "src.pipeline" not in importlib.sys.modules
    assert not any(name == "src.staging" or name.startswith("src.staging.") for name in importlib.sys.modules)
    assert not any(name == "freezing" or name.startswith("freezing.") for name in importlib.sys.modules)
    assert not any(name == "construction" or name.startswith("construction.") for name in importlib.sys.modules)

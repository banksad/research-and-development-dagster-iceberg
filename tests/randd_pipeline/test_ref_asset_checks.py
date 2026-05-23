from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import scenario_path

SCENARIO_ID = "mapping_cell_number_minimal"
ASSET_KEY = dagster.AssetKey(["ref", "cell_number_mapper"])


def _run_config() -> dict:
    scenario = scenario_path(SCENARIO_ID)
    return {"ops": {"cell_number_mapper": {"config": {"cell_number_mapper_csv_path": str(scenario / "cell_number_mapper.csv")}}}}


def _build_defs(resource):
    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    checks_mod = importlib.import_module("src.randd_pipeline.checks.ref_asset_checks")
    return dagster.Definitions(
        assets=[getattr(mapping_mod, "cell_number_mapper")],
        asset_checks=[
            getattr(checks_mod, "cell_number_mapper_table_exists"),
            getattr(checks_mod, "cell_number_mapper_non_empty"),
            getattr(checks_mod, "cell_number_mapper_required_columns"),
            getattr(checks_mod, "cell_number_mapper_unique_cellnumber"),
            getattr(checks_mod, "cell_number_mapper_cellnumber_range"),
        ],
        resources={"table_store": resource},
    )


def _messages(result):
    return [e.event_specific_data.description for e in result.get_asset_check_evaluations()]


def test_cell_number_ref_checks_pass_after_materialisation(tmp_path):
    defs = _build_defs(TableStoreResource(catalog_name="cn-ref-pass", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse")))
    assert defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_run_config()).success
    assert defs.get_asset_checks_def(ASSET_KEY).execute_in_process().success


def test_cell_number_ref_checks_fail_when_missing(tmp_path):
    defs = _build_defs(TableStoreResource(catalog_name="cn-ref-missing", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse")))
    res = defs.get_asset_checks_def(ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("does not exist" in m for m in _messages(res))


def test_cell_number_ref_checks_fail_for_missing_columns(tmp_path):
    resource = TableStoreResource(catalog_name="cn-ref-cols", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(refs.REF_CELL_NUMBER_MAPPER, pd.DataFrame({"cellnumber": [1]}))
    res = _build_defs(resource).get_asset_checks_def(ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("Missing required columns" in m for m in _messages(res))


def test_cell_number_ref_checks_fail_for_duplicate_rows(tmp_path):
    resource = TableStoreResource(catalog_name="cn-ref-dupes", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    df = pd.DataFrame({"cellnumber": [1, 1], "uni_count": [10, 10], "uni_employment": [11, 11]})
    resource.get_table_store().create_table_from_dataframe(refs.REF_CELL_NUMBER_MAPPER, df)
    res = _build_defs(resource).get_asset_checks_def(ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("duplicate row(s)" in m for m in _messages(res))


def test_cell_number_ref_checks_fail_for_range(tmp_path):
    resource = TableStoreResource(catalog_name="cn-ref-range", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    df = pd.DataFrame({"cellnumber": [818], "uni_count": [10], "uni_employment": [11]})
    resource.get_table_store().create_table_from_dataframe(refs.REF_CELL_NUMBER_MAPPER, df)
    res = _build_defs(resource).get_asset_checks_def(ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("inclusive range 1..817" in m for m in _messages(res))


def test_cell_number_ref_checks_attach_to_asset_key():
    checks_mod = importlib.import_module("src.randd_pipeline.checks.ref_asset_checks")
    assert getattr(checks_mod, "cell_number_mapper_table_exists").asset_key == ASSET_KEY

from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import scenario_path

FOREIGN_SCENARIO_ID = "mapping_foreign_ownership_minimal"
CELL_NUMBER_SCENARIO_ID = "mapping_cell_number_minimal"
MAPPED_ASSET_KEY = dagster.AssetKey(["intermediate", "mapped_responses"])
CELL_NUMBER_ASSET_KEY = dagster.AssetKey(["intermediate", "cell_number_mapped_responses"])


def _build_defs(resource):
    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    checks_mod = importlib.import_module("src.randd_pipeline.checks.mapping_asset_checks")
    return dagster.Definitions(
        assets=[getattr(mapping_mod, "mapped_responses")],
        asset_checks=[
            getattr(checks_mod, "mapped_responses_table_exists"),
            getattr(checks_mod, "mapped_responses_non_empty"),
            getattr(checks_mod, "mapped_responses_required_columns"),
            getattr(checks_mod, "mapped_responses_unique_grain"),
            getattr(checks_mod, "mapped_responses_ultfoc_present"),
            getattr(checks_mod, "cell_number_mapped_responses_table_exists"),
            getattr(checks_mod, "cell_number_mapped_responses_non_empty"),
            getattr(checks_mod, "cell_number_mapped_responses_required_columns"),
            getattr(checks_mod, "cell_number_mapped_responses_unique_grain"),
            getattr(checks_mod, "cell_number_mapped_responses_mapping_columns_present"),
        ],
        resources={"table_store": resource},
    )


def _messages(result):
    return [e.event_specific_data.description for e in result.get_asset_check_evaluations()]


def test_mapped_checks_transition_expected_required_columns_failure_after_materialisation(tmp_path):
    scenario = scenario_path(FOREIGN_SCENARIO_ID)
    resource = TableStoreResource(catalog_name="mapped-pass", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES, pd.read_csv(scenario / "staged_responses.csv"))
    resource.get_table_store().create_table_from_dataframe(refs.REF_ULTFOC_MAPPER, pd.read_csv(scenario / "ultfoc_mapper.csv"))
    defs = _build_defs(resource)
    assert defs.get_implicit_global_asset_job_def().execute_in_process().success
    result = defs.get_asset_checks_def(MAPPED_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not result.success
    messages = _messages(result)
    # Transition expectation: required-columns check targets canonical v1 mapped contract,
    # while runtime mapped_responses is still foreign-ownership-only until consolidation.
    assert any("Missing required columns" in m for m in messages)
    assert not any("contains duplicate row(s)" in m for m in messages)
    assert not any("null/blank" in m for m in messages)




def test_mapped_required_columns_check_passes_for_canonical_v1_fixture(tmp_path):
    scenario = scenario_path(CELL_NUMBER_SCENARIO_ID)
    resource = TableStoreResource(catalog_name="mapped-v1-cols", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(
        refs.INTERMEDIATE_MAPPED_RESPONSES,
        pd.read_csv(scenario / "expected_cell_number_mapped_responses.csv"),
    )
    res = _build_defs(resource).get_asset_checks_def(MAPPED_ASSET_KEY).execute_in_process()
    assert res.success

def test_mapped_checks_fail_when_table_missing(tmp_path):
    resource = TableStoreResource(catalog_name="mapped-missing", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    res = _build_defs(resource).get_asset_checks_def(MAPPED_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("does not exist" in m for m in _messages(res))


def test_mapped_checks_fail_for_missing_columns(tmp_path):
    resource = TableStoreResource(catalog_name="mapped-cols", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, pd.DataFrame({"reference": ["A"]}))
    res = _build_defs(resource).get_asset_checks_def(MAPPED_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("Missing required columns" in m for m in _messages(res))


def test_mapped_checks_fail_for_duplicate_grain(tmp_path):
    scenario = scenario_path(FOREIGN_SCENARIO_ID)
    df = pd.read_csv(scenario / "expected_mapped_responses.csv")
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    resource = TableStoreResource(catalog_name="mapped-dupes", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, df)
    res = _build_defs(resource).get_asset_checks_def(MAPPED_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("duplicate row(s)" in m for m in _messages(res))


def test_mapped_checks_fail_for_null_blank_ultfoc(tmp_path):
    scenario = scenario_path(FOREIGN_SCENARIO_ID)
    df = pd.read_csv(scenario / "expected_mapped_responses.csv")
    df.loc[0, "ultfoc"] = "   "
    resource = TableStoreResource(catalog_name="mapped-ultfoc", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, df)
    res = _build_defs(resource).get_asset_checks_def(MAPPED_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("null/blank" in m for m in _messages(res))


def test_mapped_checks_attach_to_asset_key():
    checks_mod = importlib.import_module("src.randd_pipeline.checks.mapping_asset_checks")
    assert getattr(checks_mod, "mapped_responses_table_exists").asset_key == MAPPED_ASSET_KEY


def test_cell_number_mapped_checks_pass_for_fixture_table(tmp_path):
    scenario = scenario_path(CELL_NUMBER_SCENARIO_ID)
    resource = TableStoreResource(catalog_name="cn-map-pass", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(
        refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES,
        pd.read_csv(scenario / "expected_cell_number_mapped_responses.csv"),
    )
    assert _build_defs(resource).get_asset_checks_def(CELL_NUMBER_ASSET_KEY).execute_in_process().success


def test_cell_number_mapped_checks_fail_when_table_missing(tmp_path):
    resource = TableStoreResource(catalog_name="cn-map-missing", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    res = _build_defs(resource).get_asset_checks_def(CELL_NUMBER_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("does not exist" in m for m in _messages(res))


def test_cell_number_mapped_checks_fail_for_missing_columns(tmp_path):
    resource = TableStoreResource(catalog_name="cn-map-cols", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES, pd.DataFrame({"reference": ["A"]}))
    res = _build_defs(resource).get_asset_checks_def(CELL_NUMBER_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("Missing required columns" in m for m in _messages(res))


def test_cell_number_mapped_checks_fail_for_duplicate_grain(tmp_path):
    scenario = scenario_path(CELL_NUMBER_SCENARIO_ID)
    df = pd.read_csv(scenario / "expected_cell_number_mapped_responses.csv")
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    resource = TableStoreResource(catalog_name="cn-map-dupes", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES, df)
    res = _build_defs(resource).get_asset_checks_def(CELL_NUMBER_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("duplicate row(s)" in m for m in _messages(res))


def test_cell_number_mapped_checks_fail_for_missing_metadata_when_cellno_non_null(tmp_path):
    scenario = scenario_path(CELL_NUMBER_SCENARIO_ID)
    df = pd.read_csv(scenario / "expected_cell_number_mapped_responses.csv")
    df.loc[0, "cellno"] = 1
    df.loc[0, "uni_count"] = "   "
    resource = TableStoreResource(catalog_name="cn-map-meta", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES, df)
    res = _build_defs(resource).get_asset_checks_def(CELL_NUMBER_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not res.success
    assert any("non-null cellno" in m for m in _messages(res))


def test_cell_number_mapped_checks_attach_to_asset_key():
    checks_mod = importlib.import_module("src.randd_pipeline.checks.mapping_asset_checks")
    assert getattr(checks_mod, "cell_number_mapped_responses_table_exists").asset_key == CELL_NUMBER_ASSET_KEY

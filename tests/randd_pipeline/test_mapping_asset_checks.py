from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import scenario_path

SCENARIO_ID = "mapping_foreign_ownership_minimal"
MAPPED_RESPONSES_ASSET_KEY = dagster.AssetKey(["intermediate", "mapped_responses"])


def _mapping_run_config() -> dict:
    scenario = scenario_path(SCENARIO_ID)
    return {
        "ops": {
            "mapped_responses": {
                "config": {
                    "ultfoc_mapper_csv_path": str(scenario / "ultfoc_mapper.csv"),
                }
            }
        }
    }


def _build_defs(resource: TableStoreResource):
    mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
    checks_mod = importlib.import_module("src.randd_pipeline.checks.mapping_asset_checks")

    return dagster.Definitions(
        assets=[getattr(mapping_mod, "ultfoc_mapper"), getattr(mapping_mod, "mapped_responses")],
        asset_checks=[
            getattr(checks_mod, "mapped_responses_table_exists"),
            getattr(checks_mod, "mapped_responses_non_empty"),
            getattr(checks_mod, "mapped_responses_required_columns"),
            getattr(checks_mod, "mapped_responses_unique_grain"),
            getattr(checks_mod, "mapped_responses_ultfoc_present"),
        ],
        resources={"table_store": resource},
    )


def _check_messages(check_result) -> list[str]:
    return [event.event_specific_data.description for event in check_result.get_asset_check_evaluations()]


def _seed_staged_responses_and_mapper(store) -> None:
    scenario = scenario_path(SCENARIO_ID)
    staged = pd.read_csv(scenario / "staged_responses.csv")
    mapper = pd.read_csv(scenario / "ultfoc_mapper.csv")
    store.create_table_from_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES, staged)
    store.create_table_from_dataframe(refs.REF_ULTFOC_MAPPER, mapper)


def test_mapped_asset_checks_pass_after_materialisation(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="mapped-check-pass", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)
    _seed_staged_responses_and_mapper(resource.get_table_store())

    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_mapping_run_config())
    assert result.success

    check_result = defs.get_asset_checks_def(MAPPED_RESPONSES_ASSET_KEY).execute_in_process()
    assert check_result.success


def test_mapped_asset_checks_fail_clearly_when_table_missing(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="mapped-check-missing", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    check_result = defs.get_asset_checks_def(MAPPED_RESPONSES_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("does not exist" in message for message in _check_messages(check_result))


def test_mapped_required_columns_check_fails_clearly_when_columns_missing(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="mapped-check-columns", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    store = resource.get_table_store()
    store.create_table_from_dataframe(
        refs.INTERMEDIATE_MAPPED_RESPONSES,
        pd.DataFrame({"reference": ["A"], "instance": [1], "ultfoc": ["GB"]}),
    )

    check_result = defs.get_asset_checks_def(MAPPED_RESPONSES_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("Missing required columns: survey_year, survey_type" in message for message in _check_messages(check_result))


def test_mapped_unique_grain_check_fails_clearly_when_duplicates_present(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="mapped-check-dupes", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    duplicate_df = pd.DataFrame(
        {
            "reference": ["A", "A"],
            "instance": [1, 1],
            "survey_type": ["BERD", "BERD"],
            "survey_year": [2024, 2024],
            "ultfoc": ["US", "US"],
        }
    )
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, duplicate_df)

    check_result = defs.get_asset_checks_def(MAPPED_RESPONSES_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("duplicate row(s)" in message for message in _check_messages(check_result))


def test_mapped_ultfoc_check_fails_clearly_when_values_null_or_blank(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="mapped-check-ultfoc", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)

    df = pd.DataFrame(
        {
            "reference": ["A", "B"],
            "instance": [1, 2],
            "survey_type": ["BERD", "BERD"],
            "survey_year": [2024, 2024],
            "ultfoc": [None, "   "],
        }
    )
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, df)

    check_result = defs.get_asset_checks_def(MAPPED_RESPONSES_ASSET_KEY).execute_in_process(raise_on_error=False)
    assert not check_result.success
    assert any("null/blank values" in message for message in _check_messages(check_result))


def test_mapped_checks_attach_to_explicit_asset_key() -> None:
    checks_mod = importlib.import_module("src.randd_pipeline.checks.mapping_asset_checks")
    assert getattr(checks_mod, "mapped_responses_table_exists").asset_key == MAPPED_RESPONSES_ASSET_KEY


def test_mapped_checks_do_not_import_legacy_modules(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="mapped-check-import-smoke", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = _build_defs(resource)
    _seed_staged_responses_and_mapper(resource.get_table_store())

    result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=_mapping_run_config())
    assert result.success

    check_result = defs.get_asset_checks_def(MAPPED_RESPONSES_ASSET_KEY).execute_in_process()
    assert check_result.success
    assert not any(name == "src.mapping" or name.startswith("src.mapping.") for name in importlib.sys.modules)
    assert "src.pipeline" not in importlib.sys.modules
    assert not any(name == "src.staging" or name.startswith("src.staging.") for name in importlib.sys.modules)
    assert not any(name == "freezing" or name.startswith("freezing.") for name in importlib.sys.modules)
    assert not any(name == "construction" or name.startswith("construction.") for name in importlib.sys.modules)

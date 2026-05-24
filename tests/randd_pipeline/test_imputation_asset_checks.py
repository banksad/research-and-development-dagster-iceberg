from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

CHECK_NAMES = [
    "imputed_responses_table_exists",
    "imputed_responses_non_empty",
    "imputed_responses_required_columns",
    "imputed_responses_unique_grain",
    "imputed_responses_imputation_marker_populated",
    "imputed_responses_no_illegal_missing_imputed_values",
]


def _run_checks(tmp_path, df=None):
    resource = TableStoreResource(catalog_name="imputation-checks", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    if df is not None:
        store.create_table_from_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES, df, overwrite=False)

    mod = importlib.import_module("src.randd_pipeline.checks.imputation_asset_checks")
    checks = [getattr(mod, n) for n in CHECK_NAMES]
    defs = dagster.Definitions(asset_checks=checks, resources={"table_store": resource})
    return defs.get_implicit_global_asset_job_def().execute_in_process(), checks


def _event_descriptions(result):
    return [evt.event_specific_data.asset_check_evaluation.description for evt in result.get_asset_check_evaluations()]


def test_imputation_asset_checks_pass_on_expected_fixture_and_attach_imputed_asset_key(tmp_path) -> None:
    expected = load_scenario_csv("mapping_to_imputation_minimal", "expected_imputed_responses.csv")
    result, checks = _run_checks(tmp_path, expected)
    assert result.success
    expected_key = dagster.AssetKey(["intermediate", "imputed_responses"])
    assert all(check.asset_key == expected_key for check in checks)


def test_imputation_asset_checks_missing_table_failure(tmp_path) -> None:
    result, _ = _run_checks(tmp_path, None)
    assert not result.success
    assert any("does not exist" in d for d in _event_descriptions(result))


def test_imputation_asset_checks_missing_required_columns_failure(tmp_path) -> None:
    df = load_scenario_csv("mapping_to_imputation_minimal", "expected_imputed_responses.csv").drop(columns=["imp_marker"])
    result, _ = _run_checks(tmp_path, df)
    assert not result.success
    assert any("imp_marker" in d for d in _event_descriptions(result))


def test_imputation_asset_checks_duplicate_grain_failure(tmp_path) -> None:
    df = load_scenario_csv("mapping_to_imputation_minimal", "expected_imputed_responses.csv")
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    result, _ = _run_checks(tmp_path, df)
    assert not result.success
    assert any("duplicate" in d.lower() for d in _event_descriptions(result))


def test_imputation_asset_checks_null_or_blank_imp_marker_failure(tmp_path) -> None:
    df = load_scenario_csv("mapping_to_imputation_minimal", "expected_imputed_responses.csv")
    df.loc[df.index[0], "imp_marker"] = None
    df.loc[df.index[1], "imp_marker"] = "   "
    result, _ = _run_checks(tmp_path, df)
    assert not result.success
    assert any("imp_marker" in d for d in _event_descriptions(result))


def test_imputation_asset_checks_illegal_missing_imputed_value_failure(tmp_path) -> None:
    df = load_scenario_csv("mapping_to_imputation_minimal", "expected_imputed_responses.csv")
    df.loc[df.index[0], "imp_marker"] = "TMI"
    df.loc[df.index[0], "601_imputed"] = None
    result, _ = _run_checks(tmp_path, df)
    assert not result.success
    assert any("illegal missing" in d.lower() for d in _event_descriptions(result))


def test_imputation_runtime_and_check_modules_do_not_import_legacy_modules() -> None:
    importlib.import_module("src.randd_pipeline.assets.imputation")
    importlib.import_module("src.randd_pipeline.checks.imputation_checks")
    importlib.import_module("src.randd_pipeline.checks.imputation_asset_checks")
    banned_prefixes = ["src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]
    assert not any(any(name == pref or name.startswith(pref + ".") for pref in banned_prefixes) for name in importlib.sys.modules)

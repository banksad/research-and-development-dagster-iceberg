from __future__ import annotations

import importlib

import pytest

dagster = pytest.importorskip("dagster")
pd = pytest.importorskip("pandas")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


ALL_CHECKS = [
    "outlier_adjusted_responses_table_exists",
    "outlier_adjusted_responses_non_empty",
    "outlier_adjusted_responses_required_columns",
    "outlier_adjusted_responses_unique_grain",
    "outlier_adjusted_responses_outlier_flag_populated",
    "outlier_adjusted_responses_outlier_source_populated",
    "outlier_adjusted_responses_manual_adjustment_reason_present",
]


def _resource(tmp_path, suffix: str) -> TableStoreResource:
    return TableStoreResource(catalog_name=f"outlier-check-{suffix}", catalog_type="local_sql", warehouse=str(tmp_path / suffix))


def _seed_expected(resource: TableStoreResource):
    df = load_scenario_csv("imputation_to_outlier_minimal", "expected_outlier_adjusted_responses.csv")
    resource.get_table_store().create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, df, overwrite=False)
    return df


def test_all_outlier_adjusted_checks_pass_on_expected_fixture(tmp_path) -> None:
    resource = _resource(tmp_path, "pass")
    _seed_expected(resource)
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    for check_name in ALL_CHECKS:
        assert getattr(mod, check_name)(resource).passed


def test_all_checks_attach_to_expected_asset_key() -> None:
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    expected = dagster.AssetKey(["intermediate", "outlier_adjusted_responses"])
    for check_name in ALL_CHECKS:
        assert getattr(mod, check_name).asset_key == expected


def test_missing_table_failure(tmp_path) -> None:
    resource = _resource(tmp_path, "missing")
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    assert not mod.outlier_adjusted_responses_non_empty(resource).passed


def test_missing_required_columns_failure(tmp_path) -> None:
    resource = _resource(tmp_path, "required")
    df = _seed_expected(resource).drop(columns=["outlier"])
    store = resource.get_table_store()
    store.drop_table(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, df, overwrite=False)
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    assert not mod.outlier_adjusted_responses_required_columns(resource).passed


def test_duplicate_grain_failure(tmp_path) -> None:
    resource = _resource(tmp_path, "dupe")
    df = _seed_expected(resource)
    dupe = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    store = resource.get_table_store()
    store.drop_table(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, dupe, overwrite=False)
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    assert not mod.outlier_adjusted_responses_unique_grain(resource).passed


def test_null_outlier_failure(tmp_path) -> None:
    resource = _resource(tmp_path, "null-outlier")
    df = _seed_expected(resource)
    df.loc[0, "outlier"] = None
    store = resource.get_table_store()
    store.drop_table(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, df, overwrite=False)
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    assert not mod.outlier_adjusted_responses_outlier_flag_populated(resource).passed


def test_null_or_blank_outlier_source_failure(tmp_path) -> None:
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    for value in [None, "   "]:
        resource = _resource(tmp_path, f"source-{str(value)}")
        df = _seed_expected(resource)
        df.loc[0, "outlier_source"] = value
        store = resource.get_table_store()
        store.drop_table(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
        store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, df, overwrite=False)
        assert not mod.outlier_adjusted_responses_outlier_source_populated(resource).passed


def test_invalid_outlier_source_failure(tmp_path) -> None:
    resource = _resource(tmp_path, "bad-source")
    df = _seed_expected(resource)
    df.loc[0, "outlier_source"] = "invalid"
    store = resource.get_table_store()
    store.drop_table(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, df, overwrite=False)
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    assert not mod.outlier_adjusted_responses_outlier_source_populated(resource).passed


def test_missing_manual_adjustment_reason_failure(tmp_path) -> None:
    resource = _resource(tmp_path, "missing-reason")
    df = _seed_expected(resource)
    idx = df.index[df["outlier_adjustment_applied"]][0]
    df.loc[idx, "outlier_reason"] = ""
    store = resource.get_table_store()
    store.drop_table(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, df, overwrite=False)
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    assert not mod.outlier_adjusted_responses_manual_adjustment_reason_present(resource).passed

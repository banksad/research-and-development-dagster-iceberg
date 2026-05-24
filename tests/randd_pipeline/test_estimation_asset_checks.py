from __future__ import annotations

import importlib

import pytest

dagster = pytest.importorskip("dagster")
pd = pytest.importorskip("pandas")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def _build_resource(tmp_path, df=None):
    resource = TableStoreResource(catalog_name="est-check", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    if df is not None:
        store.create_table_from_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES, df, overwrite=False)
    return resource


def test_estimation_asset_checks_asset_key_and_pass_fixture(tmp_path) -> None:
    mod = importlib.import_module("src.randd_pipeline.checks.estimation_asset_checks")
    resource = _build_resource(tmp_path, load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv"))
    assert mod._ASSET_KEY == dagster.AssetKey(["intermediate", "estimated_responses"])
    checks = [
        mod.estimated_responses_table_exists,
        mod.estimated_responses_non_empty,
        mod.estimated_responses_required_columns,
        mod.estimated_responses_unique_grain,
        mod.estimated_responses_weights_populated,
        mod.estimated_responses_weights_positive,
    ]
    for check in checks:
        assert check.asset_key == dagster.AssetKey(["intermediate", "estimated_responses"])
        assert check(resource).passed


def test_estimation_asset_checks_negative_cases(tmp_path) -> None:
    mod = importlib.import_module("src.randd_pipeline.checks.estimation_asset_checks")

    missing = _build_resource(tmp_path / "missing", None)
    assert not mod.estimated_responses_table_exists(missing).passed

    df = load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv")
    assert not mod.estimated_responses_required_columns(_build_resource(tmp_path / "required", df.drop(columns=["a_weight"]))).passed
    assert not mod.estimated_responses_unique_grain(_build_resource(tmp_path / "dupes", pd.concat([df, df.iloc[[0]]], ignore_index=True))).passed

    null_a = df.copy(); null_a.loc[0, "a_weight"] = None
    assert not mod.estimated_responses_weights_populated(_build_resource(tmp_path / "nulla", null_a)).passed
    null_g = df.copy(); null_g.loc[0, "g_weight"] = None
    assert not mod.estimated_responses_weights_populated(_build_resource(tmp_path / "nullg", null_g)).passed

    bad = df.copy(); bad.loc[0, "a_weight"] = 0; bad.loc[1, "g_weight"] = -1
    assert not mod.estimated_responses_weights_positive(_build_resource(tmp_path / "zeroneg", bad)).passed

    non_numeric = df.copy(); non_numeric.loc[0, "a_weight"] = "bad"
    assert not mod.estimated_responses_weights_positive(_build_resource(tmp_path / "nonnumeric", non_numeric)).passed

    infinite = df.copy(); infinite.loc[0, "g_weight"] = float("inf")
    assert not mod.estimated_responses_weights_positive(_build_resource(tmp_path / "infinite", infinite)).passed

from __future__ import annotations

import importlib

import pytest

dagster = pytest.importorskip("dagster")
pd = pytest.importorskip("pandas")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def _run_checks(tmp_path, df=None):
    mod = importlib.import_module("src.randd_pipeline.checks.estimation_asset_checks")
    resource = TableStoreResource(catalog_name="est-check", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    if df is not None:
        store.create_table_from_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES, df, overwrite=False)
    checks = [
        mod.estimated_responses_table_exists,
        mod.estimated_responses_non_empty,
        mod.estimated_responses_required_columns,
        mod.estimated_responses_unique_grain,
        mod.estimated_responses_weights_populated,
        mod.estimated_responses_weights_positive,
    ]
    defs = dagster.Definitions(asset_checks=checks, resources={"table_store": resource})
    return mod, defs.get_implicit_global_asset_job_def().execute_in_process()


def test_estimation_asset_checks_asset_key_and_pass_fixture(tmp_path) -> None:
    mod, _ = _run_checks(tmp_path, load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv"))
    assert mod._ASSET_KEY == dagster.AssetKey(["intermediate", "estimated_responses"])
    for check in [
        mod.estimated_responses_table_exists,
        mod.estimated_responses_non_empty,
        mod.estimated_responses_required_columns,
        mod.estimated_responses_unique_grain,
        mod.estimated_responses_weights_populated,
        mod.estimated_responses_weights_positive,
    ]:
        assert check.asset_key == dagster.AssetKey(["intermediate", "estimated_responses"])


def test_estimation_asset_checks_negative_cases(tmp_path) -> None:
    _, res_missing = _run_checks(tmp_path / "missing", None)
    assert not res_missing.success

    df = load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv")
    assert not _run_checks(tmp_path / "required", df.drop(columns=["a_weight"]))[1].success
    assert not _run_checks(tmp_path / "dupes", pd.concat([df, df.iloc[[0]]], ignore_index=True))[1].success

    null_a = df.copy(); null_a.loc[0, "a_weight"] = None
    assert not _run_checks(tmp_path / "nulla", null_a)[1].success
    null_g = df.copy(); null_g.loc[0, "g_weight"] = None
    assert not _run_checks(tmp_path / "nullg", null_g)[1].success

    bad = df.copy(); bad.loc[0, "a_weight"] = 0; bad.loc[1, "g_weight"] = -1
    assert not _run_checks(tmp_path / "zeroneg", bad)[1].success

    non_numeric = df.copy(); non_numeric.loc[0, "a_weight"] = "bad"
    assert not _run_checks(tmp_path / "nonnumeric", non_numeric)[1].success

    infinite = df.copy(); infinite.loc[0, "g_weight"] = float("inf")
    assert not _run_checks(tmp_path / "infinite", infinite)[1].success

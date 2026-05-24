from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv, scenario_path

dagster = pytest.importorskip("dagster")
mod = importlib.import_module("src.randd_pipeline.assets.site_apportionment")


def test_asset_keys_are_explicit():
    assert mod.site_apportionment_factors.key == dagster.AssetKey(["ref", "site_apportionment_factors"])
    assert mod.site_apportioned_responses.key == dagster.AssetKey(["intermediate", "site_apportioned_responses"])


def test_assets_materialise_expected_outputs(tmp_path):
    est = load_scenario_csv("estimation_to_site_apportionment_minimal", "input_estimated_responses.csv")
    exp = load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv")
    fac = load_scenario_csv("estimation_to_site_apportionment_minimal", "input_site_apportionment_factors.csv")
    csv_path = scenario_path("estimation_to_site_apportionment_minimal") / "input_site_apportionment_factors.csv"
    resource = TableStoreResource(catalog_name="siteap", catalog_type="local_sql", warehouse=str(tmp_path / "w"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES, est, overwrite=False)

    defs = dagster.Definitions(assets=[mod.site_apportionment_factors, mod.site_apportioned_responses], resources={"table_store": resource})
    rc = {"ops": {"site_apportionment_factors": {"config": {"site_factors_csv_path": str(csv_path)}}, "site_apportioned_responses": {"config": {"value_columns": ["211"]}}}}
    assert defs.get_implicit_global_asset_job_def().execute_in_process(run_config=rc).success
    assert_frame_equal_sorted(store.read_table_as_dataframe(refs.REF_SITE_APPORTIONMENT_FACTORS), fac, ["reference", "site_id"])
    assert_frame_equal_sorted(store.read_table_as_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES), exp, ["reference", "site_id"])


def test_configs_reject_invalid_values():
    with pytest.raises(ValueError):
        mod.SiteApportionmentFactorsInputConfig(site_factors_csv_path=" ")
    with pytest.raises(ValueError):
        mod.SiteApportionmentConfig(value_columns=[])
    with pytest.raises(ValueError):
        mod.SiteApportionmentConfig(value_columns=[" "])
    with pytest.raises(ValueError):
        mod.SiteApportionmentConfig(value_columns=["211"], output_suffix=" ")
    with pytest.raises(ValueError):
        mod.SiteApportionmentConfig(value_columns=["211"], factor_sum_tolerance=0)
    with pytest.raises(ValueError):
        mod.SiteApportionmentConfig(value_columns=["211"], factor_sum_tolerance=-1)


def test_no_legacy_imports():
    importlib.import_module("src.randd_pipeline.assets.site_apportionment")
    for forbidden in ["src.site_apportionment", "src.estimation", "src.outlier_detection", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

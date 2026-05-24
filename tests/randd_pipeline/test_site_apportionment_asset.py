from __future__ import annotations
import importlib, pytest
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv, scenario_path
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

dagster = pytest.importorskip("dagster")

def test_asset_materialises_expected(tmp_path):
    est = load_scenario_csv("estimation_to_site_apportionment_minimal", "input_estimated_responses.csv")
    exp = load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv")
    csv_path = scenario_path("estimation_to_site_apportionment_minimal") / "input_site_apportionment_factors.csv"
    resource=TableStoreResource(catalog_name="siteap", catalog_type="local_sql", warehouse=str(tmp_path/"w")); store=resource.get_table_store(); store.create_table_from_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES, est, overwrite=False)
    mod=importlib.import_module("src.randd_pipeline.assets.site_apportionment")
    defs=dagster.Definitions(assets=[mod.site_apportionment_factors, mod.site_apportioned_responses], resources={"table_store": resource})
    rc={"ops":{"site_apportionment_factors":{"config":{"site_factors_csv_path":str(csv_path)}},"site_apportioned_responses":{"config":{"value_columns":["211"]}}}}
    assert defs.get_implicit_global_asset_job_def().execute_in_process(run_config=rc).success
    out=store.read_table_as_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES)
    assert_frame_equal_sorted(out, exp, ["reference","site_id"])

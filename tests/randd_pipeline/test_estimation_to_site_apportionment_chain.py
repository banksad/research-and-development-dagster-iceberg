from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv

dagster = pytest.importorskip("dagster")
checks = importlib.import_module("src.randd_pipeline.checks.site_apportionment_asset_checks")
assets = importlib.import_module("src.randd_pipeline.assets.site_apportionment")


def test_chain_estimation_to_site_apportionment_with_full_checks(tmp_path):
    est = load_scenario_csv("estimation_to_site_apportionment_minimal", "input_estimated_responses.csv")
    fac = load_scenario_csv("estimation_to_site_apportionment_minimal", "input_site_apportionment_factors.csv")
    exp = load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv")

    resource = TableStoreResource(catalog_name="chain", catalog_type="local_sql", warehouse=str(tmp_path / "w"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES, est, overwrite=False)
    store.create_table_from_dataframe(refs.REF_SITE_APPORTIONMENT_FACTORS, fac, overwrite=False)

    defs = dagster.Definitions(assets=[assets.site_apportioned_responses], resources={"table_store": resource})
    assert defs.get_implicit_global_asset_job_def().execute_in_process(run_config={"ops": {"site_apportioned_responses": {"config": {"value_columns": ["211"]}}}}).success

    for fn in [checks.site_apportioned_responses_table_exists, checks.site_apportioned_responses_non_empty, checks.site_apportioned_responses_required_columns, checks.site_apportioned_responses_unique_site_grain, checks.site_apportioned_responses_site_identifier_populated, checks.site_apportioned_responses_site_proportion_populated, checks.site_apportioned_responses_apportioned_values_non_negative, checks.site_apportionment_factors_table_exists, checks.site_apportionment_factors_required_columns, checks.site_apportionment_factors_unique_site_grain, checks.site_apportionment_factors_site_identifier_populated, checks.site_apportionment_factors_proportions_sum_to_one]:
        assert fn(resource).passed

    out = store.read_table_as_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES)
    assert_frame_equal_sorted(out, exp, ["reference", "site_id"])

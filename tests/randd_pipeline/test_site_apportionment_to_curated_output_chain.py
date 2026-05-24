from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv

assets_mod = importlib.import_module("src.randd_pipeline.assets.outputs")
checks_mod = importlib.import_module("src.randd_pipeline.checks.curated_statistics_asset_checks")


def test_site_to_curated_chain(tmp_path):
    resource = TableStoreResource(catalog_name="curated-chain", catalog_type="local_sql", warehouse=str(tmp_path / "w"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv"), overwrite=False)
    defs = dagster.Definitions(assets=[assets_mod.curated_rnd_statistics], asset_checks=[checks_mod.curated_rnd_statistics_table_exists, checks_mod.curated_rnd_statistics_non_empty, checks_mod.curated_rnd_statistics_required_columns, checks_mod.curated_rnd_statistics_unique_grain, checks_mod.curated_rnd_statistics_output_measure_populated, checks_mod.curated_rnd_statistics_output_value_non_negative, checks_mod.curated_rnd_statistics_provenance_columns_present, checks_mod.curated_rnd_statistics_reconciles_to_site_input], resources={"table_store": resource})
    assert defs.get_implicit_global_asset_job_def().execute_in_process().success
    assert all(x.passed for x in defs.get_implicit_global_asset_job_def().execute_in_process(asset_selection=["curated/rnd_statistics"]).get_asset_check_evaluations())
    assert_frame_equal_sorted(store.read_table_as_dataframe(refs.CURATED_RND_STATISTICS), load_scenario_csv("site_apportionment_to_curated_output_minimal", "expected_curated_rnd_statistics.csv"), ["survey_year", "survey_type", "output_measure"])

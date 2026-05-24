from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

mod = importlib.import_module("src.randd_pipeline.checks.curated_statistics_asset_checks")


def test_asset_key_and_missing_table(tmp_path):
    r = TableStoreResource(catalog_name="curated-check", catalog_type="local_sql", warehouse=str(tmp_path / "w"))
    assert mod.curated_rnd_statistics_table_exists.asset_key == dagster.AssetKey(["curated", "rnd_statistics"])
    assert not mod.curated_rnd_statistics_table_exists(r).passed


def test_asset_checks_pass_and_fail(tmp_path):
    r = TableStoreResource(catalog_name="curated-check2", catalog_type="local_sql", warehouse=str(tmp_path / "w")); s = r.get_table_store()
    curated = load_scenario_csv("site_apportionment_to_curated_output_minimal", "expected_curated_rnd_statistics.csv")
    site = load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv")
    s.create_table_from_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, site, overwrite=False)
    s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, curated, overwrite=False)
    for fn in [mod.curated_rnd_statistics_non_empty, mod.curated_rnd_statistics_required_columns, mod.curated_rnd_statistics_unique_grain, mod.curated_rnd_statistics_output_measure_populated, mod.curated_rnd_statistics_output_value_non_negative, mod.curated_rnd_statistics_provenance_columns_present, mod.curated_rnd_statistics_reconciles_to_site_input]:
        assert fn(r).passed

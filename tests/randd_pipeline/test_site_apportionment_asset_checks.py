from __future__ import annotations

import importlib
import pytest

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


dagster = pytest.importorskip("dagster")
mod = importlib.import_module("src.randd_pipeline.checks.site_apportionment_asset_checks")


def _resource(tmp_path):
    return TableStoreResource(catalog_name="checks", catalog_type="local_sql", warehouse=str(tmp_path / "w"))


def test_check_assets_attach_to_expected_keys():
    assert mod.site_apportioned_responses_non_empty.asset_key == dagster.AssetKey(["intermediate", "site_apportioned_responses"])
    assert mod.site_apportionment_factors_required_columns.asset_key == dagster.AssetKey(["ref", "site_apportionment_factors"])


def test_site_and_factor_checks_pass(tmp_path):
    r = _resource(tmp_path); s = r.get_table_store()
    s.create_table_from_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv"), overwrite=False)
    s.create_table_from_dataframe(refs.REF_SITE_APPORTIONMENT_FACTORS, load_scenario_csv("estimation_to_site_apportionment_minimal", "input_site_apportionment_factors.csv"), overwrite=False)
    for fn in [mod.site_apportioned_responses_table_exists, mod.site_apportioned_responses_non_empty, mod.site_apportioned_responses_required_columns, mod.site_apportioned_responses_unique_site_grain, mod.site_apportioned_responses_site_identifier_populated, mod.site_apportioned_responses_site_proportion_populated, mod.site_apportioned_responses_apportioned_values_non_negative, mod.site_apportionment_factors_table_exists, mod.site_apportionment_factors_required_columns, mod.site_apportionment_factors_unique_site_grain, mod.site_apportionment_factors_site_identifier_populated, mod.site_apportionment_factors_proportions_sum_to_one]:
        assert fn(r).passed

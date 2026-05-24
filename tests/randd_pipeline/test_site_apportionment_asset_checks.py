from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

dagster = pytest.importorskip("dagster")
mod = importlib.import_module("src.randd_pipeline.checks.site_apportionment_asset_checks")


def _resource(tmp_path):
    return TableStoreResource(catalog_name="checks", catalog_type="local_sql", warehouse=str(tmp_path / "w"))


def _seed(store, table, df):
    store.create_table_from_dataframe(table, df, overwrite=False)


def test_asset_keys_attached():
    site_checks = [mod.site_apportioned_responses_table_exists, mod.site_apportioned_responses_non_empty, mod.site_apportioned_responses_required_columns, mod.site_apportioned_responses_unique_site_grain, mod.site_apportioned_responses_site_identifier_populated, mod.site_apportioned_responses_site_proportion_populated, mod.site_apportioned_responses_apportioned_values_non_negative]
    factor_checks = [mod.site_apportionment_factors_table_exists, mod.site_apportionment_factors_required_columns, mod.site_apportionment_factors_unique_site_grain, mod.site_apportionment_factors_site_identifier_populated, mod.site_apportionment_factors_proportions_sum_to_one]
    assert all(c.asset_key == dagster.AssetKey(["intermediate", "site_apportioned_responses"]) for c in site_checks)
    assert all(c.asset_key == dagster.AssetKey(["ref", "site_apportionment_factors"]) for c in factor_checks)


def test_all_checks_pass_on_fixtures(tmp_path):
    r = _resource(tmp_path); s = r.get_table_store()
    _seed(s, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv"))
    _seed(s, refs.REF_SITE_APPORTIONMENT_FACTORS, load_scenario_csv("estimation_to_site_apportionment_minimal", "input_site_apportionment_factors.csv"))
    for fn in [mod.site_apportioned_responses_table_exists, mod.site_apportioned_responses_non_empty, mod.site_apportioned_responses_required_columns, mod.site_apportioned_responses_unique_site_grain, mod.site_apportioned_responses_site_identifier_populated, mod.site_apportioned_responses_site_proportion_populated, mod.site_apportioned_responses_apportioned_values_non_negative, mod.site_apportionment_factors_table_exists, mod.site_apportionment_factors_required_columns, mod.site_apportionment_factors_unique_site_grain, mod.site_apportionment_factors_site_identifier_populated, mod.site_apportionment_factors_proportions_sum_to_one]:
        assert fn(r).passed


def test_negative_site_apportioned_asset_checks(tmp_path):
    r = _resource(tmp_path); s = r.get_table_store(); base = load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv")
    assert not mod.site_apportioned_responses_table_exists(r).passed
    for fn, df in [
        (mod.site_apportioned_responses_required_columns, base.drop(columns=["site_id"])),
        (mod.site_apportioned_responses_unique_site_grain, pd.concat([base, base.iloc[[0]]], ignore_index=True)),
    ]:
        _seed(s, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, df); assert not fn(r).passed; s.delete_table(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES)
    for col, val, fn in [("site_id", None, mod.site_apportioned_responses_site_identifier_populated), ("site_id", " ", mod.site_apportioned_responses_site_identifier_populated), ("site_proportion", None, mod.site_apportioned_responses_site_proportion_populated), ("site_proportion", "x", mod.site_apportioned_responses_site_proportion_populated), ("site_proportion", -0.2, mod.site_apportioned_responses_site_proportion_populated), ("211_apportioned", None, mod.site_apportioned_responses_apportioned_values_non_negative), ("211_apportioned", "x", mod.site_apportioned_responses_apportioned_values_non_negative), ("211_apportioned", -1, mod.site_apportioned_responses_apportioned_values_non_negative)]:
        df = base.copy(); df.loc[0, col] = val; _seed(s, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, df); assert not fn(r).passed; s.delete_table(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES)


def test_negative_factor_asset_checks(tmp_path):
    r = _resource(tmp_path); s = r.get_table_store(); base = load_scenario_csv("estimation_to_site_apportionment_minimal", "input_site_apportionment_factors.csv")
    assert not mod.site_apportionment_factors_table_exists(r).passed
    _seed(s, refs.REF_SITE_APPORTIONMENT_FACTORS, base.drop(columns=["site_id"])); assert not mod.site_apportionment_factors_required_columns(r).passed; s.delete_table(refs.REF_SITE_APPORTIONMENT_FACTORS)
    _seed(s, refs.REF_SITE_APPORTIONMENT_FACTORS, pd.concat([base, base.iloc[[0]]], ignore_index=True)); assert not mod.site_apportionment_factors_unique_site_grain(r).passed; s.delete_table(refs.REF_SITE_APPORTIONMENT_FACTORS)
    for v in [None, " "]:
        df = base.copy(); df.loc[0, "site_id"] = v; _seed(s, refs.REF_SITE_APPORTIONMENT_FACTORS, df); assert not mod.site_apportionment_factors_site_identifier_populated(r).passed; s.delete_table(refs.REF_SITE_APPORTIONMENT_FACTORS)
    df = base.copy(); df.loc[df["reference"] == "SYN001", "site_proportion"] = [0.2, 0.2]; _seed(s, refs.REF_SITE_APPORTIONMENT_FACTORS, df); assert not mod.site_apportionment_factors_proportions_sum_to_one(r).passed; s.delete_table(refs.REF_SITE_APPORTIONMENT_FACTORS)
    df = base.copy(); df.loc[0, "site_proportion"] = "x"; _seed(s, refs.REF_SITE_APPORTIONMENT_FACTORS, df); assert not mod.site_apportionment_factors_proportions_sum_to_one(r).passed

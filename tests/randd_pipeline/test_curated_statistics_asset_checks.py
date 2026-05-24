from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

mod = importlib.import_module("src.randd_pipeline.checks.curated_statistics_asset_checks")


def _resource(tmp_path, name):
    return TableStoreResource(catalog_name=name, catalog_type="local_sql", warehouse=str(tmp_path / "w"))


def test_all_checks_attach_to_curated_asset_key():
    expected = dagster.AssetKey(["curated", "rnd_statistics"])
    for fn in [
        mod.curated_rnd_statistics_table_exists,
        mod.curated_rnd_statistics_non_empty,
        mod.curated_rnd_statistics_required_columns,
        mod.curated_rnd_statistics_unique_grain,
        mod.curated_rnd_statistics_output_measure_populated,
        mod.curated_rnd_statistics_output_value_non_negative,
        mod.curated_rnd_statistics_provenance_columns_present,
        mod.curated_rnd_statistics_reconciles_to_site_input,
    ]:
        assert fn.asset_key == expected


def test_all_curated_checks_pass_with_seeded_tables(tmp_path):
    r = _resource(tmp_path, "curated-check-pass"); s = r.get_table_store()
    s.create_table_from_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv"), overwrite=False)
    s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, load_scenario_csv("site_apportionment_to_curated_output_minimal", "expected_curated_rnd_statistics.csv"), overwrite=False)
    for fn in [mod.curated_rnd_statistics_table_exists, mod.curated_rnd_statistics_non_empty, mod.curated_rnd_statistics_required_columns, mod.curated_rnd_statistics_unique_grain, mod.curated_rnd_statistics_output_measure_populated, mod.curated_rnd_statistics_output_value_non_negative, mod.curated_rnd_statistics_provenance_columns_present, mod.curated_rnd_statistics_reconciles_to_site_input]:
        assert fn(r).passed


def test_negative_asset_check_cases(tmp_path):
    curated = load_scenario_csv("site_apportionment_to_curated_output_minimal", "expected_curated_rnd_statistics.csv")
    site = load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv")

    r = _resource(tmp_path, "missing-curated")
    assert not mod.curated_rnd_statistics_table_exists(r).passed

    r = _resource(tmp_path, "required-columns"); s = r.get_table_store(); s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, curated.drop(columns=["output_value"]), overwrite=False)
    assert not mod.curated_rnd_statistics_required_columns(r).passed

    r = _resource(tmp_path, "unique-grain"); s = r.get_table_store(); s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, pd.concat([curated, curated.iloc[[0]]], ignore_index=True), overwrite=False)
    assert not mod.curated_rnd_statistics_unique_grain(r).passed

    r = _resource(tmp_path, "output-measure"); s = r.get_table_store(); bad = curated.copy(); bad.loc[0, "output_measure"] = " "; s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, bad, overwrite=False)
    assert not mod.curated_rnd_statistics_output_measure_populated(r).passed

    r = _resource(tmp_path, "output-value"); s = r.get_table_store(); bad = curated.copy(); bad.loc[0, "output_value"] = -1; s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, bad, overwrite=False)
    assert not mod.curated_rnd_statistics_output_value_non_negative(r).passed

    r = _resource(tmp_path, "provenance"); s = r.get_table_store(); s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, curated.drop(columns=["pipeline_run_id"]), overwrite=False)
    assert not mod.curated_rnd_statistics_provenance_columns_present(r).passed

    r = _resource(tmp_path, "missing-site"); s = r.get_table_store(); s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, curated, overwrite=False)
    assert not mod.curated_rnd_statistics_reconciles_to_site_input(r).passed

    r = _resource(tmp_path, "changed-total"); s = r.get_table_store(); bad = curated.copy(); bad.loc[0, "output_value"] = 999; s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, bad, overwrite=False); s.create_table_from_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, site, overwrite=False)
    assert not mod.curated_rnd_statistics_reconciles_to_site_input(r).passed

    r = _resource(tmp_path, "site-nonnumeric"); s = r.get_table_store(); bad_site = site.copy(); bad_site.loc[0, "211_apportioned"] = "x"; s.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, curated, overwrite=False); s.create_table_from_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, bad_site, overwrite=False)
    assert not mod.curated_rnd_statistics_reconciles_to_site_input(r).passed

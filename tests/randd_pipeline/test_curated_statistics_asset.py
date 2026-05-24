from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv

mod = importlib.import_module("src.randd_pipeline.assets.outputs")


def test_asset_key_and_defaults():
    assert mod.curated_rnd_statistics.key == dagster.AssetKey(["curated", "rnd_statistics"])
    assert mod.CuratedRndStatisticsConfig().measure_columns == {"211_apportioned": "total_211_apportioned"}


def test_materialises_expected_output(tmp_path):
    resource = TableStoreResource(catalog_name="curated", catalog_type="local_sql", warehouse=str(tmp_path / "w"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, load_scenario_csv("site_apportionment_to_curated_output_minimal", "input_site_apportioned_responses.csv"), overwrite=False)
    defs = dagster.Definitions(assets=[mod.curated_rnd_statistics], resources={"table_store": resource})
    assert defs.get_implicit_global_asset_job_def().execute_in_process().success
    assert_frame_equal_sorted(store.read_table_as_dataframe(refs.CURATED_RND_STATISTICS), load_scenario_csv("site_apportionment_to_curated_output_minimal", "expected_curated_rnd_statistics.csv"), ["survey_year", "survey_type", "output_measure"])


def test_config_validation_and_no_legacy_imports():
    with pytest.raises(ValueError): mod.CuratedRndStatisticsConfig(measure_columns={})
    with pytest.raises(ValueError): mod.CuratedRndStatisticsConfig(measure_columns={" ": "x"})
    with pytest.raises(ValueError): mod.CuratedRndStatisticsConfig(measure_columns={"x": " "})
    importlib.import_module("src.randd_pipeline.assets.outputs")
    for forbidden in ["src.outputs", "src.site_apportionment", "src.estimation", "src.outlier_detection", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

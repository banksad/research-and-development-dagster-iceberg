from __future__ import annotations

import importlib

import pytest

dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def test_outlier_asset_check_table_exists_fails_when_missing(tmp_path) -> None:
    resource = TableStoreResource(catalog_name="outlier-check", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    result = getattr(mod, "outlier_adjusted_responses_table_exists")(resource)
    assert not result.passed


def test_outlier_asset_check_required_columns_pass(tmp_path) -> None:
    df = load_scenario_csv("imputation_to_outlier_minimal", "expected_outlier_adjusted_responses.csv")
    resource = TableStoreResource(catalog_name="outlier-check2", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, df, overwrite=False)
    mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
    assert getattr(mod, "outlier_adjusted_responses_required_columns")(resource).passed

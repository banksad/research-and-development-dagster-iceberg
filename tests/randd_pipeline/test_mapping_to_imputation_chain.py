from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def test_mapping_to_imputation_chain_smoke(tmp_path) -> None:
    mapped = load_scenario_csv("mapping_to_imputation_minimal", "mapped_responses.csv")
    resource = TableStoreResource(catalog_name="mapping-imputation-chain", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    store = resource.get_table_store()
    store.create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, mapped, overwrite=False)

    asset_mod = importlib.import_module("src.randd_pipeline.assets.imputation")
    check_mod = importlib.import_module("src.randd_pipeline.checks.imputation_asset_checks")
    defs = dagster.Definitions(
        assets=[getattr(asset_mod, "imputed_responses")],
        asset_checks=[
            getattr(check_mod, "imputed_responses_table_exists"),
            getattr(check_mod, "imputed_responses_non_empty"),
            getattr(check_mod, "imputed_responses_required_columns"),
            getattr(check_mod, "imputed_responses_unique_grain"),
            getattr(check_mod, "imputed_responses_imputation_marker_populated"),
            getattr(check_mod, "imputed_responses_no_illegal_missing_imputed_values"),
        ],
        resources={"table_store": resource},
    )
    result = defs.get_implicit_global_asset_job_def().execute_in_process()
    assert result.success

from __future__ import annotations

import importlib

import pandas as pd
import pytest

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import (
    assert_frame_equal_sorted,
    scenario_path,
)

SCENARIO_ID = "basic_responses"

dagster = pytest.importorskip("dagster")


def test_raw_full_responses_asset_materialises_fixture_to_local_table_store(tmp_path) -> None:
    raw_path = scenario_path(SCENARIO_ID) / "raw_full_responses.csv"
    expected_path = scenario_path(SCENARIO_ID) / "expected_raw_full_responses.csv"

    resource = TableStoreResource(
        catalog_name="raw-input-asset-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )

    inputs_mod = importlib.import_module("src.randd_pipeline.assets.inputs")
    raw_full_responses_asset = getattr(inputs_mod, "raw_full_responses")

    defs = dagster.Definitions(
        assets=[raw_full_responses_asset],
        resources={"table_store": resource},
    )

    result = defs.get_implicit_global_asset_job_def().execute_in_process(
        run_config={
            "ops": {
                "raw_full_responses": {
                    "config": {
                        "csv_path": str(raw_path),
                    }
                }
            }
        }
    )

    assert result.success

    store = resource.get_table_store()
    actual = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES)
    expected = pd.read_csv(expected_path)

    assert_frame_equal_sorted(actual, expected, sort_by=["reference", "instance"])


def test_raw_input_asset_smoke_does_not_import_legacy_pipeline_or_staging_modules(tmp_path) -> None:
    raw_path = scenario_path(SCENARIO_ID) / "raw_full_responses.csv"

    resource = TableStoreResource(
        catalog_name="raw-input-asset-import-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )

    inputs_mod = importlib.import_module("src.randd_pipeline.assets.inputs")
    raw_full_responses_asset = getattr(inputs_mod, "raw_full_responses")

    defs = dagster.Definitions(
        assets=[raw_full_responses_asset],
        resources={"table_store": resource},
    )

    result = defs.get_implicit_global_asset_job_def().execute_in_process(
        run_config={
            "ops": {
                "raw_full_responses": {
                    "config": {
                        "csv_path": str(raw_path),
                    }
                }
            }
        }
    )

    assert result.success
    assert "src.pipeline" not in importlib.sys.modules
    assert not any(name == "src.staging" or name.startswith("src.staging.") for name in importlib.sys.modules)

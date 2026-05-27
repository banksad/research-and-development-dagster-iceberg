"""Tests for local synthetic v1 Dagster UI demo readiness artifacts."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_local_demo_definitions_register_table_store_resource(monkeypatch):
    pytest.importorskip("dagster")

    monkeypatch.setenv("RND_PIPELINE_LOCAL_WAREHOUSE", ".tmp/custom-warehouse")

    from src.randd_pipeline import local_demo_definitions

    assert hasattr(local_demo_definitions, "defs")
    assert "table_store" in local_demo_definitions.defs.resources

    table_store_resource = local_demo_definitions.defs.resources["table_store"]
    assert table_store_resource.catalog_name == "local-refoundation-ui"
    assert table_store_resource.catalog_type == "local_sql"
    assert table_store_resource.warehouse == ".tmp/custom-warehouse"


def test_local_demo_definitions_build_implicit_global_asset_job_def():
    pytest.importorskip("dagster")

    from src.randd_pipeline.local_demo_definitions import defs

    assert defs.get_implicit_global_asset_job_def() is not None


def test_local_demo_definitions_include_expected_full_synthetic_v1_assets():
    dagster = pytest.importorskip("dagster")

    from src.randd_pipeline import local_demo_definitions

    expected_asset_keys = {
        dagster.AssetKey(["raw", "full_responses"]),
        dagster.AssetKey(["intermediate", "staged_responses"]),
        dagster.AssetKey(["ref", "ultfoc_mapper"]),
        dagster.AssetKey(["ref", "cell_number_mapper"]),
        dagster.AssetKey(["ops", "manual_outliers"]),
        dagster.AssetKey(["ref", "site_apportionment_factors"]),
        dagster.AssetKey(["intermediate", "mapped_responses"]),
        dagster.AssetKey(["intermediate", "imputed_responses"]),
        dagster.AssetKey(["intermediate", "outlier_adjusted_responses"]),
        dagster.AssetKey(["intermediate", "estimated_responses"]),
        dagster.AssetKey(["intermediate", "site_apportioned_responses"]),
        dagster.AssetKey(["curated", "rnd_statistics"]),
    }

    asset_graph = local_demo_definitions.defs.resolve_asset_graph()
    available_asset_keys = set(asset_graph.get_all_asset_keys())

    assert expected_asset_keys <= available_asset_keys


def test_full_synthetic_v1_run_config_is_fixture_only_and_expected_shape():
    config_path = Path("config/dagster/full_synthetic_v1_run_config.yaml")
    assert config_path.exists()

    yaml = pytest.importorskip("yaml")
    payload = yaml.safe_load(config_path.read_text())
    ops = payload.get("ops", {})

    expected_ops = {
        "raw_full_responses": ["csv_path"],
        "staged_responses": ["contributors_csv_path", "responses_long_csv_path"],
        "ultfoc_mapper": ["ultfoc_mapper_csv_path"],
        "cell_number_mapper": ["cell_number_mapper_csv_path"],
        "manual_outliers": ["manual_outliers_csv_path"],
        "site_apportionment_factors": ["site_factors_csv_path"],
    }

    assert set(ops) == set(expected_ops)

    for op_name, config_keys in expected_ops.items():
        op_config = ops[op_name]["config"]
        assert set(op_config) == set(config_keys)
        for value in op_config.values():
            assert value.startswith("tests/fixtures/synthetic/")
            assert ".." not in value
            assert not value.startswith("/")
            assert "gs://" not in value
            assert "s3://" not in value
            assert "secret" not in value.lower()
            assert "prod" not in value.lower()


def test_full_synthetic_v1_run_config_ops_are_available_in_local_demo_definitions():
    pytest.importorskip("dagster")
    yaml = pytest.importorskip("yaml")

    from src.randd_pipeline import local_demo_definitions

    config_path = Path("config/dagster/full_synthetic_v1_run_config.yaml")
    payload = yaml.safe_load(config_path.read_text())
    configured_ops = set(payload.get("ops", {}))

    available_op_names = {asset_def.op.name for asset_def in local_demo_definitions.defs.assets}

    assert configured_ops <= available_op_names

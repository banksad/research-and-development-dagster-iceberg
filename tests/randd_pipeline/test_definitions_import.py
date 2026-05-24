"""Import smoke tests for lean refoundation Dagster definitions."""

import pytest

from src.randd_pipeline import definitions


def test_randd_pipeline_definitions_importable():
    assert hasattr(definitions, "defs")
    assert hasattr(definitions.defs, "assets")


def test_default_definitions_register_canonical_mapped_responses_check_set():
    pytest.importorskip("dagster")
    check_names = {check_def.name for check_def in definitions.defs.asset_checks}

    assert "mapped_responses_table_exists" in check_names
    assert "mapped_responses_non_empty" in check_names
    assert "mapped_responses_required_columns" in check_names
    assert "mapped_responses_unique_grain" in check_names
    assert "mapped_responses_ultfoc_present" in check_names
    assert "mapped_responses_cell_number_mapping_complete" in check_names


def test_default_definitions_include_estimated_asset_and_checks():
    dagster = pytest.importorskip("dagster")
    asset_key_paths = {tuple(asset_def.key.path) for asset_def in definitions.defs.assets}
    check_names = {check_def.name for check_def in definitions.defs.asset_checks}

    assert ("intermediate", "estimated_responses") in asset_key_paths
    assert "estimated_responses_table_exists" in check_names
    assert "estimated_responses_non_empty" in check_names
    assert "estimated_responses_required_columns" in check_names
    assert "estimated_responses_unique_grain" in check_names
    assert "estimated_responses_weights_populated" in check_names
    assert "estimated_responses_weights_positive" in check_names


def test_default_definitions_expose_production_facing_dependency_edges():
    dagster = pytest.importorskip("dagster")
    assets_by_key = {tuple(asset_def.key.path): asset_def for asset_def in definitions.defs.assets}

    def dependency_paths(asset_key_path: tuple[str, ...]) -> set[tuple[str, ...]]:
        deps = assets_by_key[asset_key_path].dependency_keys
        return {tuple(dep.path) for dep in deps}

    assert ("raw", "full_responses") in dependency_paths(("intermediate", "staged_responses"))
    assert {
        ("intermediate", "staged_responses"),
        ("ref", "ultfoc_mapper"),
        ("ref", "cell_number_mapper"),
    }.issubset(dependency_paths(("intermediate", "mapped_responses")))
    assert ("intermediate", "mapped_responses") in dependency_paths(("intermediate", "imputed_responses"))
    assert ("intermediate", "imputed_responses") in dependency_paths(("intermediate", "outlier_adjusted_responses"))
    assert ("intermediate", "outlier_adjusted_responses") in dependency_paths(("intermediate", "estimated_responses"))


def test_default_definitions_exclude_cell_number_checkpoint_asset_and_checks():
    pytest.importorskip("dagster")
    asset_key_paths = {tuple(asset_def.key.path) for asset_def in definitions.defs.assets}
    check_names = {check_def.name for check_def in definitions.defs.asset_checks}

    assert ("intermediate", "cell_number_mapped_responses") not in asset_key_paths
    assert "cell_number_mapped_responses_table_exists" not in check_names


def test_default_definitions_exclude_manual_outliers_csv_loader_asset():
    """Default defs assume ops.manual_outliers can be managed operationally outside CSV loading."""
    pytest.importorskip("dagster")
    asset_key_paths = {tuple(asset_def.key.path) for asset_def in definitions.defs.assets}
    assert ("ops", "manual_outliers") not in asset_key_paths

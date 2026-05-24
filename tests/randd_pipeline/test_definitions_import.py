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


def test_default_definitions_exclude_cell_number_checkpoint_asset_and_checks():
    pytest.importorskip("dagster")
    asset_key_paths = {tuple(asset_def.key.path) for asset_def in definitions.defs.assets}
    check_names = {check_def.name for check_def in definitions.defs.asset_checks}

    assert ("intermediate", "cell_number_mapped_responses") not in asset_key_paths
    assert "cell_number_mapped_responses_table_exists" not in check_names
    assert "cell_number_mapped_responses_non_empty" not in check_names
    assert "cell_number_mapped_responses_required_columns" not in check_names
    assert "cell_number_mapped_responses_unique_grain" not in check_names
    assert "cell_number_mapped_responses_mapping_columns_present" not in check_names


def test_default_definitions_include_imputed_responses_asset():
    pytest.importorskip("dagster")
    asset_key_paths = {tuple(asset_def.key.path) for asset_def in definitions.defs.assets}
    assert ("intermediate", "imputed_responses") in asset_key_paths


def test_default_definitions_register_imputed_responses_check_set():
    pytest.importorskip("dagster")
    check_names = {check_def.name for check_def in definitions.defs.asset_checks}

    assert "imputed_responses_table_exists" in check_names
    assert "imputed_responses_non_empty" in check_names
    assert "imputed_responses_required_columns" in check_names
    assert "imputed_responses_unique_grain" in check_names
    assert "imputed_responses_imputation_marker_populated" in check_names
    assert "imputed_responses_no_illegal_missing_imputed_values" in check_names


def test_default_definitions_include_outlier_adjusted_responses_asset():
    pytest.importorskip("dagster")
    asset_key_paths = {tuple(asset_def.key.path) for asset_def in definitions.defs.assets}
    assert ("intermediate", "outlier_adjusted_responses") in asset_key_paths


def test_default_definitions_register_outlier_adjusted_responses_check_set():
    pytest.importorskip("dagster")
    check_names = {check_def.name for check_def in definitions.defs.asset_checks}

    assert "outlier_adjusted_responses_table_exists" in check_names
    assert "outlier_adjusted_responses_non_empty" in check_names
    assert "outlier_adjusted_responses_required_columns" in check_names
    assert "outlier_adjusted_responses_unique_grain" in check_names
    assert "outlier_adjusted_responses_outlier_flag_populated" in check_names
    assert "outlier_adjusted_responses_outlier_source_populated" in check_names
    assert "outlier_adjusted_responses_manual_adjustment_reason_present" in check_names


def test_default_definitions_exclude_manual_outliers_csv_loader_asset():
    """Default defs assume ops.manual_outliers can be managed operationally outside CSV loading."""
    pytest.importorskip("dagster")
    asset_key_paths = {tuple(asset_def.key.path) for asset_def in definitions.defs.assets}
    assert ("ops", "manual_outliers") not in asset_key_paths

"""Minimal importable Dagster definitions for the lean refoundation package.

This module intentionally exposes only a minimal placeholder Definitions object.
It must remain importable regardless of whether Dagster is installed.
"""

from __future__ import annotations

from typing import Callable

try:
    from dagster import Definitions
    from src.randd_pipeline.assets.inputs import raw_full_responses
    from src.randd_pipeline.assets.imputation import imputed_responses
    from src.randd_pipeline.assets.outliers import outlier_adjusted_responses
    from src.randd_pipeline.assets.estimation import estimated_responses
    from src.randd_pipeline.assets.mapping import (
        cell_number_mapper,
        mapped_responses,
        ultfoc_mapper,
    )
    from src.randd_pipeline.assets.staging import staged_responses
    from src.randd_pipeline.checks.raw_input_checks import (
        raw_full_responses_non_empty,
        raw_full_responses_required_columns,
        raw_full_responses_table_exists,
    )
    from src.randd_pipeline.checks.imputation_asset_checks import (
        imputed_responses_imputation_marker_populated,
        imputed_responses_no_illegal_missing_imputed_values,
        imputed_responses_non_empty,
        imputed_responses_required_columns,
        imputed_responses_table_exists,
        imputed_responses_unique_grain,
    )
    from src.randd_pipeline.checks.outlier_asset_checks import (
        outlier_adjusted_responses_manual_adjustment_reason_present,
        outlier_adjusted_responses_non_empty,
        outlier_adjusted_responses_outlier_flag_populated,
        outlier_adjusted_responses_outlier_source_populated,
        outlier_adjusted_responses_required_columns,
        outlier_adjusted_responses_table_exists,
        outlier_adjusted_responses_unique_grain,
    )
    from src.randd_pipeline.checks.estimation_asset_checks import (
        estimated_responses_non_empty,
        estimated_responses_required_columns,
        estimated_responses_table_exists,
        estimated_responses_unique_grain,
        estimated_responses_weights_populated,
        estimated_responses_weights_positive,
    )
    from src.randd_pipeline.checks.mapping_asset_checks import (
        mapped_responses_non_empty,
        mapped_responses_required_columns,
        mapped_responses_table_exists,
        mapped_responses_cell_number_mapping_complete,
        mapped_responses_ultfoc_present,
        mapped_responses_unique_grain,
    )
    from src.randd_pipeline.checks.ref_asset_checks import (
        cell_number_mapper_cellnumber_range,
        cell_number_mapper_non_empty,
        cell_number_mapper_required_columns,
        cell_number_mapper_table_exists,
        cell_number_mapper_unique_cellnumber,
        ultfoc_mapper_non_empty,
        ultfoc_mapper_required_columns,
        ultfoc_mapper_table_exists,
        ultfoc_mapper_unique_ruref,
    )
    from src.randd_pipeline.checks.staging_asset_checks import (
        staged_responses_non_empty,
        staged_responses_required_columns,
        staged_responses_table_exists,
        staged_responses_unique_grain,
    )
except ModuleNotFoundError:  # pragma: no cover - fallback for environments without dagster
    class Definitions:  # type: ignore[override]
        """Lightweight fallback so this module remains importable without Dagster."""

        def __init__(
            self,
            assets: list[Callable] | None = None,
            asset_checks: list[Callable] | None = None,
        ):
            self.assets = assets or []
            self.asset_checks = asset_checks or []


    _assets: list[Callable] = []
    _asset_checks: list[Callable] = []
else:
    _assets = [
        raw_full_responses,
        staged_responses,
        ultfoc_mapper,
        mapped_responses,
        imputed_responses,
        outlier_adjusted_responses,
        estimated_responses,
        cell_number_mapper,
    ]
    _asset_checks = [
        raw_full_responses_table_exists,
        raw_full_responses_non_empty,
        raw_full_responses_required_columns,
        staged_responses_table_exists,
        staged_responses_non_empty,
        staged_responses_required_columns,
        staged_responses_unique_grain,
        ultfoc_mapper_table_exists,
        ultfoc_mapper_non_empty,
        ultfoc_mapper_required_columns,
        ultfoc_mapper_unique_ruref,
        mapped_responses_table_exists,
        mapped_responses_non_empty,
        mapped_responses_required_columns,
        mapped_responses_unique_grain,
        mapped_responses_ultfoc_present,
        mapped_responses_cell_number_mapping_complete,
        imputed_responses_table_exists,
        imputed_responses_non_empty,
        imputed_responses_required_columns,
        imputed_responses_unique_grain,
        imputed_responses_imputation_marker_populated,
        imputed_responses_no_illegal_missing_imputed_values,
        outlier_adjusted_responses_table_exists,
        outlier_adjusted_responses_non_empty,
        outlier_adjusted_responses_required_columns,
        outlier_adjusted_responses_unique_grain,
        outlier_adjusted_responses_outlier_flag_populated,
        outlier_adjusted_responses_outlier_source_populated,
        outlier_adjusted_responses_manual_adjustment_reason_present,
        estimated_responses_table_exists,
        estimated_responses_non_empty,
        estimated_responses_required_columns,
        estimated_responses_unique_grain,
        estimated_responses_weights_populated,
        estimated_responses_weights_positive,
        cell_number_mapper_table_exists,
        cell_number_mapper_non_empty,
        cell_number_mapper_required_columns,
        cell_number_mapper_unique_cellnumber,
        cell_number_mapper_cellnumber_range,
    ]
    # NOTE: cell_number_mapped_responses remains available as a temporary
    # checkpoint/debug asset but is intentionally excluded from default
    # production-facing Dagster definitions.


defs = Definitions(assets=_assets, asset_checks=_asset_checks)

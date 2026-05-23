"""Minimal importable Dagster definitions for the lean refoundation package.

This module intentionally exposes only a minimal placeholder Definitions object.
It must remain importable regardless of whether Dagster is installed.
"""

from __future__ import annotations

from typing import Callable

try:
    from dagster import Definitions
    from src.randd_pipeline.assets.inputs import raw_full_responses
    from src.randd_pipeline.assets.mapping import mapped_responses, ultfoc_mapper
    from src.randd_pipeline.assets.staging import staged_responses
    from src.randd_pipeline.checks.raw_input_checks import (
        raw_full_responses_non_empty,
        raw_full_responses_required_columns,
        raw_full_responses_table_exists,
    )
    from src.randd_pipeline.checks.mapping_asset_checks import (
        mapped_responses_non_empty,
        mapped_responses_required_columns,
        mapped_responses_table_exists,
        mapped_responses_ultfoc_present,
        mapped_responses_unique_grain,
    )
    from src.randd_pipeline.checks.ref_asset_checks import (
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
    _assets = [raw_full_responses, staged_responses, ultfoc_mapper, mapped_responses]
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
    ]


defs = Definitions(assets=_assets, asset_checks=_asset_checks)

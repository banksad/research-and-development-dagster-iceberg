"""Dagster asset checks for the staged responses smoke asset/table."""

from __future__ import annotations

from src.randd_pipeline.checks.staging_checks import (
    check_staged_responses_non_empty,
    check_staged_responses_required_columns,
    check_staged_responses_unique_grain,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover - dagster optional in this package
    pass

else:
    _STAGED_RESPONSES_ASSET_KEY = AssetKey(["intermediate", "staged_responses"])

    @asset_check(asset=_STAGED_RESPONSES_ASSET_KEY, name="table_exists")
    def staged_responses_table_exists(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        exists = store.table_exists(refs.INTERMEDIATE_STAGED_RESPONSES)

        if exists:
            return AssetCheckResult(passed=True, description="Table intermediate.staged_responses exists.")

        return AssetCheckResult(passed=False, description="Table intermediate.staged_responses does not exist.")

    @asset_check(asset=_STAGED_RESPONSES_ASSET_KEY, name="non_empty")
    def staged_responses_non_empty(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_STAGED_RESPONSES):
            return AssetCheckResult(
                passed=False,
                description="Table intermediate.staged_responses does not exist, cannot validate non-empty check.",
            )

        df = store.read_table_as_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES)
        passed, message = check_staged_responses_non_empty(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_STAGED_RESPONSES_ASSET_KEY, name="required_columns")
    def staged_responses_required_columns(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_STAGED_RESPONSES):
            return AssetCheckResult(
                passed=False,
                description="Table intermediate.staged_responses does not exist, cannot validate required columns.",
            )

        df = store.read_table_as_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES)
        passed, message = check_staged_responses_required_columns(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_STAGED_RESPONSES_ASSET_KEY, name="unique_grain")
    def staged_responses_unique_grain(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_STAGED_RESPONSES):
            return AssetCheckResult(
                passed=False,
                description="Table intermediate.staged_responses does not exist, cannot validate grain uniqueness.",
            )

        df = store.read_table_as_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES)
        passed, message = check_staged_responses_unique_grain(df)
        return AssetCheckResult(passed=passed, description=message)

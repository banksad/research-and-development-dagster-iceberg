"""Dagster asset checks for the mapped responses smoke asset/table."""

from __future__ import annotations

from src.randd_pipeline.checks.mapping_checks import (
    check_mapped_responses_non_empty,
    check_mapped_responses_required_columns,
    check_mapped_responses_ultfoc_present,
    check_mapped_responses_unique_grain,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover - dagster optional in this package
    pass

else:
    _MAPPED_RESPONSES_ASSET_KEY = AssetKey(["intermediate", "mapped_responses"])

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="table_exists")
    def mapped_responses_table_exists(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        exists = store.table_exists(refs.INTERMEDIATE_MAPPED_RESPONSES)

        if exists:
            return AssetCheckResult(passed=True, description="Table intermediate.mapped_responses exists.")

        return AssetCheckResult(passed=False, description="Table intermediate.mapped_responses does not exist.")

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="non_empty")
    def mapped_responses_non_empty(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_MAPPED_RESPONSES):
            return AssetCheckResult(
                passed=False,
                description="Table intermediate.mapped_responses does not exist, cannot validate non-empty check.",
            )

        df = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
        passed, message = check_mapped_responses_non_empty(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="required_columns")
    def mapped_responses_required_columns(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_MAPPED_RESPONSES):
            return AssetCheckResult(
                passed=False,
                description="Table intermediate.mapped_responses does not exist, cannot validate required columns.",
            )

        df = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
        passed, message = check_mapped_responses_required_columns(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="unique_grain")
    def mapped_responses_unique_grain(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_MAPPED_RESPONSES):
            return AssetCheckResult(
                passed=False,
                description="Table intermediate.mapped_responses does not exist, cannot validate grain uniqueness.",
            )

        df = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
        passed, message = check_mapped_responses_unique_grain(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="ultfoc_present")
    def mapped_responses_ultfoc_present(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_MAPPED_RESPONSES):
            return AssetCheckResult(
                passed=False,
                description="Table intermediate.mapped_responses does not exist, cannot validate ultfoc presence.",
            )

        df = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
        passed, message = check_mapped_responses_ultfoc_present(df)
        return AssetCheckResult(passed=passed, description=message)

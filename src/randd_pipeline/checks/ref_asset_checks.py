"""Dagster asset checks for the ``ref.ultfoc_mapper`` reference table."""

from __future__ import annotations

from src.randd_pipeline.checks.ref_checks import (
    check_ultfoc_mapper_non_empty,
    check_ultfoc_mapper_required_columns,
    check_ultfoc_mapper_unique_ruref,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover - dagster optional in this package
    pass

else:
    _ULTFOC_MAPPER_ASSET_KEY = AssetKey(["ref", "ultfoc_mapper"])

    @asset_check(asset=_ULTFOC_MAPPER_ASSET_KEY, name="table_exists")
    def ultfoc_mapper_table_exists(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        exists = store.table_exists(refs.REF_ULTFOC_MAPPER)

        if exists:
            return AssetCheckResult(passed=True, description="Table ref.ultfoc_mapper exists.")

        return AssetCheckResult(passed=False, description="Table ref.ultfoc_mapper does not exist.")

    @asset_check(asset=_ULTFOC_MAPPER_ASSET_KEY, name="non_empty")
    def ultfoc_mapper_non_empty(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.REF_ULTFOC_MAPPER):
            return AssetCheckResult(
                passed=False,
                description="Table ref.ultfoc_mapper does not exist, cannot validate non-empty check.",
            )

        df = store.read_table_as_dataframe(refs.REF_ULTFOC_MAPPER)
        passed, message = check_ultfoc_mapper_non_empty(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_ULTFOC_MAPPER_ASSET_KEY, name="required_columns")
    def ultfoc_mapper_required_columns(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.REF_ULTFOC_MAPPER):
            return AssetCheckResult(
                passed=False,
                description="Table ref.ultfoc_mapper does not exist, cannot validate required columns.",
            )

        df = store.read_table_as_dataframe(refs.REF_ULTFOC_MAPPER)
        passed, message = check_ultfoc_mapper_required_columns(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_ULTFOC_MAPPER_ASSET_KEY, name="unique_ruref")
    def ultfoc_mapper_unique_ruref(table_store: TableStoreResource) -> AssetCheckResult:
        store = table_store.get_table_store()
        if not store.table_exists(refs.REF_ULTFOC_MAPPER):
            return AssetCheckResult(
                passed=False,
                description="Table ref.ultfoc_mapper does not exist, cannot validate ruref uniqueness.",
            )

        df = store.read_table_as_dataframe(refs.REF_ULTFOC_MAPPER)
        passed, message = check_ultfoc_mapper_unique_ruref(df)
        return AssetCheckResult(passed=passed, description=message)

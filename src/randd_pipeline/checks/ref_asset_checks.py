"""Dagster asset checks for reference tables."""

from __future__ import annotations

from src.randd_pipeline.checks.ref_checks import (
    check_cell_number_mapper_cellnumber_range,
    check_cell_number_mapper_non_empty,
    check_cell_number_mapper_required_columns,
    check_cell_number_mapper_unique_cellnumber,
    check_ultfoc_mapper_non_empty,
    check_ultfoc_mapper_required_columns,
    check_ultfoc_mapper_unique_ruref,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    _ULTFOC_MAPPER_ASSET_KEY = AssetKey(["ref", "ultfoc_mapper"])
    _CELL_NUMBER_MAPPER_ASSET_KEY = AssetKey(["ref", "cell_number_mapper"])

    def _read_or_missing(table_store: TableStoreResource, table_ref: str, for_check: str):
        store = table_store.get_table_store()
        if not store.table_exists(table_ref):
            return None, AssetCheckResult(passed=False, description=f"Table {table_ref} does not exist, cannot validate {for_check}.")
        return store.read_table_as_dataframe(table_ref), None

    @asset_check(asset=_ULTFOC_MAPPER_ASSET_KEY, name="table_exists")
    def ultfoc_mapper_table_exists(table_store: TableStoreResource) -> AssetCheckResult:
        exists = table_store.get_table_store().table_exists(refs.REF_ULTFOC_MAPPER)
        return AssetCheckResult(passed=exists, description=("Table ref.ultfoc_mapper exists." if exists else "Table ref.ultfoc_mapper does not exist."))

    @asset_check(asset=_ULTFOC_MAPPER_ASSET_KEY, name="non_empty")
    def ultfoc_mapper_non_empty(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.REF_ULTFOC_MAPPER, "non-empty check")
        if missing:
            return missing
        passed, message = check_ultfoc_mapper_non_empty(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_ULTFOC_MAPPER_ASSET_KEY, name="required_columns")
    def ultfoc_mapper_required_columns(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.REF_ULTFOC_MAPPER, "required columns")
        if missing:
            return missing
        passed, message = check_ultfoc_mapper_required_columns(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_ULTFOC_MAPPER_ASSET_KEY, name="unique_ruref")
    def ultfoc_mapper_unique_ruref(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.REF_ULTFOC_MAPPER, "ruref uniqueness")
        if missing:
            return missing
        passed, message = check_ultfoc_mapper_unique_ruref(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_CELL_NUMBER_MAPPER_ASSET_KEY, name="table_exists")
    def cell_number_mapper_table_exists(table_store: TableStoreResource) -> AssetCheckResult:
        exists = table_store.get_table_store().table_exists(refs.REF_CELL_NUMBER_MAPPER)
        return AssetCheckResult(passed=exists, description=("Table ref.cell_number_mapper exists." if exists else "Table ref.cell_number_mapper does not exist."))

    @asset_check(asset=_CELL_NUMBER_MAPPER_ASSET_KEY, name="non_empty")
    def cell_number_mapper_non_empty(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.REF_CELL_NUMBER_MAPPER, "non-empty check")
        if missing:
            return missing
        passed, message = check_cell_number_mapper_non_empty(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_CELL_NUMBER_MAPPER_ASSET_KEY, name="required_columns")
    def cell_number_mapper_required_columns(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.REF_CELL_NUMBER_MAPPER, "required columns")
        if missing:
            return missing
        passed, message = check_cell_number_mapper_required_columns(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_CELL_NUMBER_MAPPER_ASSET_KEY, name="unique_cellnumber")
    def cell_number_mapper_unique_cellnumber(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.REF_CELL_NUMBER_MAPPER, "cellnumber uniqueness")
        if missing:
            return missing
        passed, message = check_cell_number_mapper_unique_cellnumber(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_CELL_NUMBER_MAPPER_ASSET_KEY, name="cellnumber_range")
    def cell_number_mapper_cellnumber_range(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.REF_CELL_NUMBER_MAPPER, "cellnumber range")
        if missing:
            return missing
        passed, message = check_cell_number_mapper_cellnumber_range(df)
        return AssetCheckResult(passed=passed, description=message)

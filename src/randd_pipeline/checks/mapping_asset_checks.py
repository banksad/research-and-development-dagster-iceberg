"""Dagster asset checks for mapping output tables."""

from __future__ import annotations

from src.randd_pipeline.checks.mapping_checks import (
    check_cell_number_mapped_responses_mapping_columns_present,
    check_cell_number_mapped_responses_non_empty,
    check_cell_number_mapped_responses_required_columns,
    check_cell_number_mapped_responses_unique_grain,
    check_mapped_responses_non_empty,
    check_mapped_responses_required_columns,
    check_mapped_responses_ultfoc_present,
    check_mapped_responses_unique_grain,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    _MAPPED_RESPONSES_ASSET_KEY = AssetKey(["intermediate", "mapped_responses"])
    _CELL_NUMBER_MAPPED_ASSET_KEY = AssetKey(["intermediate", "cell_number_mapped_responses"])

    def _read_or_missing(table_store: TableStoreResource, table_ref: str, for_check: str):
        store = table_store.get_table_store()
        if not store.table_exists(table_ref):
            return None, AssetCheckResult(passed=False, description=f"Table {table_ref} does not exist, cannot validate {for_check}.")
        return store.read_table_as_dataframe(table_ref), None

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="table_exists")
    def mapped_responses_table_exists(table_store: TableStoreResource) -> AssetCheckResult:
        exists = table_store.get_table_store().table_exists(refs.INTERMEDIATE_MAPPED_RESPONSES)
        return AssetCheckResult(passed=exists, description=("Table intermediate.mapped_responses exists." if exists else "Table intermediate.mapped_responses does not exist."))

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="non_empty")
    def mapped_responses_non_empty(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.INTERMEDIATE_MAPPED_RESPONSES, "non-empty check")
        if missing:
            return missing
        passed, message = check_mapped_responses_non_empty(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="required_columns")
    def mapped_responses_required_columns(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.INTERMEDIATE_MAPPED_RESPONSES, "required columns")
        if missing:
            return missing
        passed, message = check_mapped_responses_required_columns(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="unique_grain")
    def mapped_responses_unique_grain(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.INTERMEDIATE_MAPPED_RESPONSES, "grain uniqueness")
        if missing:
            return missing
        passed, message = check_mapped_responses_unique_grain(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_MAPPED_RESPONSES_ASSET_KEY, name="ultfoc_present")
    def mapped_responses_ultfoc_present(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.INTERMEDIATE_MAPPED_RESPONSES, "ultfoc presence")
        if missing:
            return missing
        passed, message = check_mapped_responses_ultfoc_present(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_CELL_NUMBER_MAPPED_ASSET_KEY, name="table_exists")
    def cell_number_mapped_responses_table_exists(table_store: TableStoreResource) -> AssetCheckResult:
        exists = table_store.get_table_store().table_exists(refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES)
        return AssetCheckResult(passed=exists, description=("Table intermediate.cell_number_mapped_responses exists." if exists else "Table intermediate.cell_number_mapped_responses does not exist."))

    @asset_check(asset=_CELL_NUMBER_MAPPED_ASSET_KEY, name="non_empty")
    def cell_number_mapped_responses_non_empty(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES, "non-empty check")
        if missing:
            return missing
        passed, message = check_cell_number_mapped_responses_non_empty(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_CELL_NUMBER_MAPPED_ASSET_KEY, name="required_columns")
    def cell_number_mapped_responses_required_columns(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES, "required columns")
        if missing:
            return missing
        passed, message = check_cell_number_mapped_responses_required_columns(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_CELL_NUMBER_MAPPED_ASSET_KEY, name="unique_grain")
    def cell_number_mapped_responses_unique_grain(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES, "grain uniqueness")
        if missing:
            return missing
        passed, message = check_cell_number_mapped_responses_unique_grain(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_CELL_NUMBER_MAPPED_ASSET_KEY, name="mapping_columns_present")
    def cell_number_mapped_responses_mapping_columns_present(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES, "cell-number mapped metadata")
        if missing:
            return missing
        passed, message = check_cell_number_mapped_responses_mapping_columns_present(df)
        return AssetCheckResult(passed=passed, description=message)

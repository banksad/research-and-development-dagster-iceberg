from __future__ import annotations

from src.randd_pipeline.checks.imputation_checks import (
    check_imputation_marker_populated,
    check_imputed_responses_non_empty,
    check_imputed_responses_required_columns,
    check_imputed_responses_unique_grain,
    check_no_illegal_missing_imputed_values,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    _ASSET_KEY = AssetKey(["intermediate", "imputed_responses"])

    def _read_or_missing(table_store: TableStoreResource, for_check: str):
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_IMPUTED_RESPONSES):
            return None, AssetCheckResult(passed=False, description=f"Table {refs.INTERMEDIATE_IMPUTED_RESPONSES} does not exist, cannot validate {for_check}.")
        return store.read_table_as_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES), None

    @asset_check(asset=_ASSET_KEY, name="table_exists")
    def imputed_responses_table_exists(table_store: TableStoreResource) -> AssetCheckResult:
        exists = table_store.get_table_store().table_exists(refs.INTERMEDIATE_IMPUTED_RESPONSES)
        return AssetCheckResult(passed=exists, description="Table intermediate.imputed_responses exists." if exists else "Table intermediate.imputed_responses does not exist.")

    @asset_check(asset=_ASSET_KEY, name="non_empty")
    def imputed_responses_non_empty(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, "non-empty check")
        if missing:
            return missing
        passed, message = check_imputed_responses_non_empty(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_ASSET_KEY, name="required_columns")
    def imputed_responses_required_columns(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, "required columns")
        if missing:
            return missing
        passed, message = check_imputed_responses_required_columns(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_ASSET_KEY, name="unique_grain")
    def imputed_responses_unique_grain(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, "unique grain")
        if missing:
            return missing
        passed, message = check_imputed_responses_unique_grain(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_ASSET_KEY, name="imputation_marker_populated")
    def imputed_responses_imputation_marker_populated(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, "marker populated")
        if missing:
            return missing
        passed, message = check_imputation_marker_populated(df)
        return AssetCheckResult(passed=passed, description=message)

    @asset_check(asset=_ASSET_KEY, name="no_illegal_missing_imputed_values")
    def imputed_responses_no_illegal_missing_imputed_values(table_store: TableStoreResource) -> AssetCheckResult:
        df, missing = _read_or_missing(table_store, "imputed value completeness")
        if missing:
            return missing
        passed, message = check_no_illegal_missing_imputed_values(df)
        return AssetCheckResult(passed=passed, description=message)

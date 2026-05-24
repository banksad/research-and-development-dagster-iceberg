from __future__ import annotations

from src.randd_pipeline.checks.outlier_checks import *
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    _ASSET_KEY = AssetKey(["intermediate", "outlier_adjusted_responses"])

    def _read_or_missing(table_store: TableStoreResource, for_check: str):
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES):
            return None, AssetCheckResult(passed=False, description=f"Table {refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES} does not exist, cannot validate {for_check}.")
        return store.read_table_as_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES), None

    @asset_check(asset=_ASSET_KEY, name="table_exists")
    def outlier_adjusted_responses_table_exists(table_store: TableStoreResource):
        exists = table_store.get_table_store().table_exists(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
        return AssetCheckResult(passed=exists, description="Table intermediate.outlier_adjusted_responses exists." if exists else "Table intermediate.outlier_adjusted_responses does not exist.")

    @asset_check(asset=_ASSET_KEY, name="non_empty")
    def outlier_adjusted_responses_non_empty(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "non-empty check")
        if missing: return missing
        p,m=check_outlier_adjusted_non_empty(df); return AssetCheckResult(passed=p, description=m)

    @asset_check(asset=_ASSET_KEY, name="required_columns")
    def outlier_adjusted_responses_required_columns(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "required columns")
        if missing: return missing
        p,m=check_outlier_adjusted_required_columns(df); return AssetCheckResult(passed=p, description=m)

    @asset_check(asset=_ASSET_KEY, name="unique_grain")
    def outlier_adjusted_responses_unique_grain(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "unique grain")
        if missing: return missing
        p,m=check_outlier_adjusted_unique_grain(df); return AssetCheckResult(passed=p, description=m)

    @asset_check(asset=_ASSET_KEY, name="outlier_flag_populated")
    def outlier_adjusted_responses_outlier_flag_populated(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "outlier flag populated")
        if missing: return missing
        p,m=check_outlier_flag_populated(df); return AssetCheckResult(passed=p, description=m)

    @asset_check(asset=_ASSET_KEY, name="outlier_source_populated")
    def outlier_adjusted_responses_outlier_source_populated(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "outlier_source populated")
        if missing: return missing
        p,m=check_outlier_source_populated(df); return AssetCheckResult(passed=p, description=m)

    @asset_check(asset=_ASSET_KEY, name="manual_adjustment_reason_present")
    def outlier_adjusted_responses_manual_adjustment_reason_present(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "manual adjustment reason")
        if missing: return missing
        p,m=check_manual_adjustment_reason_present(df); return AssetCheckResult(passed=p, description=m)

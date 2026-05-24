from __future__ import annotations

from src.randd_pipeline.checks.estimation_checks import *
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    _ASSET_KEY = AssetKey(["intermediate", "estimated_responses"])

    def _read_or_missing(table_store: TableStoreResource, for_check: str):
        store = table_store.get_table_store()
        if not store.table_exists(refs.INTERMEDIATE_ESTIMATED_RESPONSES):
            return None, AssetCheckResult(passed=False, description=f"Table {refs.INTERMEDIATE_ESTIMATED_RESPONSES} does not exist, cannot validate {for_check}.")
        return store.read_table_as_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES), None

    @asset_check(asset=_ASSET_KEY, name="table_exists")
    def estimated_responses_table_exists(table_store: TableStoreResource):
        exists = table_store.get_table_store().table_exists(refs.INTERMEDIATE_ESTIMATED_RESPONSES)
        return AssetCheckResult(passed=exists, description="Table intermediate.estimated_responses exists." if exists else "Table intermediate.estimated_responses does not exist.")

    @asset_check(asset=_ASSET_KEY, name="non_empty")
    def estimated_responses_non_empty(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "non-empty check");
        if missing: return missing
        p,m=check_estimated_responses_non_empty(df); return AssetCheckResult(passed=p, description=m)

    @asset_check(asset=_ASSET_KEY, name="required_columns")
    def estimated_responses_required_columns(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "required columns");
        if missing: return missing
        p,m=check_estimated_responses_required_columns(df); return AssetCheckResult(passed=p, description=m)

    @asset_check(asset=_ASSET_KEY, name="unique_grain")
    def estimated_responses_unique_grain(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "unique grain");
        if missing: return missing
        p,m=check_estimated_responses_unique_grain(df); return AssetCheckResult(passed=p, description=m)

    @asset_check(asset=_ASSET_KEY, name="weights_populated")
    def estimated_responses_weights_populated(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "weights populated");
        if missing: return missing
        p,m=check_estimation_weights_populated(df); return AssetCheckResult(passed=p, description=m)

    @asset_check(asset=_ASSET_KEY, name="weights_positive")
    def estimated_responses_weights_positive(table_store: TableStoreResource):
        df, missing = _read_or_missing(table_store, "weights positive");
        if missing: return missing
        p,m=check_estimation_weights_positive(df); return AssetCheckResult(passed=p, description=m)

from __future__ import annotations

from src.randd_pipeline.checks.site_apportionment_checks import (
    check_apportioned_values_non_negative,
    check_site_apportioned_non_empty,
    check_site_apportioned_required_columns,
    check_site_apportioned_unique_site_grain,
    check_site_factors_proportions_sum_to_one,
    check_site_factors_required_columns,
    check_site_factors_unique_site_grain,
    check_site_identifier_populated,
    check_site_proportion_populated,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    _SITE = AssetKey(["intermediate", "site_apportioned_responses"])
    _FAC = AssetKey(["ref", "site_apportionment_factors"])

    def _read(store_res: TableStoreResource, table: str, for_check: str):
        store = store_res.get_table_store()
        if not store.table_exists(table):
            return None, AssetCheckResult(passed=False, description=f"Table {table} does not exist, cannot validate {for_check}.")
        return store.read_table_as_dataframe(table), None

    @asset_check(asset=_SITE, name="table_exists")
    def site_apportioned_responses_table_exists(table_store: TableStoreResource):
        ex = table_store.get_table_store().table_exists(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES)
        return AssetCheckResult(passed=ex, description="Table intermediate.site_apportioned_responses exists." if ex else "Table intermediate.site_apportioned_responses does not exist.")

    @asset_check(asset=_SITE, name="non_empty")
    def site_apportioned_responses_non_empty(table_store: TableStoreResource):
        df, m = _read(table_store, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, "non-empty check")
        if m:
            return m
        p, d = check_site_apportioned_non_empty(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_SITE, name="required_columns")
    def site_apportioned_responses_required_columns(table_store: TableStoreResource):
        df, m = _read(table_store, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, "required columns")
        if m:
            return m
        p, d = check_site_apportioned_required_columns(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_SITE, name="unique_site_grain")
    def site_apportioned_responses_unique_site_grain(table_store: TableStoreResource):
        df, m = _read(table_store, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, "unique site grain")
        if m:
            return m
        p, d = check_site_apportioned_unique_site_grain(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_SITE, name="site_identifier_populated")
    def site_apportioned_responses_site_identifier_populated(table_store: TableStoreResource):
        df, m = _read(table_store, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, "site id population")
        if m:
            return m
        p, d = check_site_identifier_populated(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_SITE, name="site_proportion_populated")
    def site_apportioned_responses_site_proportion_populated(table_store: TableStoreResource):
        df, m = _read(table_store, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, "site proportion population")
        if m:
            return m
        p, d = check_site_proportion_populated(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_SITE, name="apportioned_values_non_negative")
    def site_apportioned_responses_apportioned_values_non_negative(table_store: TableStoreResource):
        df, m = _read(table_store, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, "apportioned values")
        if m:
            return m
        p, d = check_apportioned_values_non_negative(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_FAC, name="table_exists")
    def site_apportionment_factors_table_exists(table_store: TableStoreResource):
        ex = table_store.get_table_store().table_exists(refs.REF_SITE_APPORTIONMENT_FACTORS)
        return AssetCheckResult(passed=ex, description="Table ref.site_apportionment_factors exists." if ex else "Table ref.site_apportionment_factors does not exist.")

    @asset_check(asset=_FAC, name="required_columns")
    def site_apportionment_factors_required_columns(table_store: TableStoreResource):
        df, m = _read(table_store, refs.REF_SITE_APPORTIONMENT_FACTORS, "required columns")
        if m:
            return m
        p, d = check_site_factors_required_columns(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_FAC, name="unique_site_grain")
    def site_apportionment_factors_unique_site_grain(table_store: TableStoreResource):
        df, m = _read(table_store, refs.REF_SITE_APPORTIONMENT_FACTORS, "unique site grain")
        if m:
            return m
        p, d = check_site_factors_unique_site_grain(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_FAC, name="site_identifier_populated")
    def site_apportionment_factors_site_identifier_populated(table_store: TableStoreResource):
        df, m = _read(table_store, refs.REF_SITE_APPORTIONMENT_FACTORS, "site id population")
        if m:
            return m
        p, d = check_site_identifier_populated(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_FAC, name="proportions_sum_to_one")
    def site_apportionment_factors_proportions_sum_to_one(table_store: TableStoreResource):
        df, m = _read(table_store, refs.REF_SITE_APPORTIONMENT_FACTORS, "proportion sums")
        if m:
            return m
        p, d = check_site_factors_proportions_sum_to_one(df)
        return AssetCheckResult(passed=p, description=d)

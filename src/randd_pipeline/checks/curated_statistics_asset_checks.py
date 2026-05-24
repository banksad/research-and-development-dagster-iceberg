from __future__ import annotations

from src.randd_pipeline.checks.asset_check_helpers import read_table_for_check
from src.randd_pipeline.checks.curated_statistics_checks import (
    check_curated_rnd_statistics_non_empty,
    check_curated_rnd_statistics_output_measure_populated,
    check_curated_rnd_statistics_output_value_non_negative,
    check_curated_rnd_statistics_provenance_columns_present,
    check_curated_rnd_statistics_reconciles_to_site_input,
    check_curated_rnd_statistics_required_columns,
    check_curated_rnd_statistics_unique_grain,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult, AssetKey, asset_check
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    _CURATED = AssetKey(["curated", "rnd_statistics"])

    @asset_check(asset=_CURATED, name="table_exists")
    def curated_rnd_statistics_table_exists(table_store: TableStoreResource):
        ex = table_store.get_table_store().table_exists(refs.CURATED_RND_STATISTICS)
        return AssetCheckResult(passed=ex, description="Table curated.rnd_statistics exists." if ex else "Table curated.rnd_statistics does not exist.")

    @asset_check(asset=_CURATED, name="non_empty")
    def curated_rnd_statistics_non_empty(table_store: TableStoreResource):
        df, m = read_table_for_check(table_store, refs.CURATED_RND_STATISTICS, check_label="non-empty check")
        if m:
            return m
        p, d = check_curated_rnd_statistics_non_empty(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_CURATED, name="required_columns")
    def curated_rnd_statistics_required_columns(table_store: TableStoreResource):
        df, m = read_table_for_check(table_store, refs.CURATED_RND_STATISTICS, check_label="required columns")
        if m:
            return m
        p, d = check_curated_rnd_statistics_required_columns(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_CURATED, name="unique_grain")
    def curated_rnd_statistics_unique_grain(table_store: TableStoreResource):
        df, m = read_table_for_check(table_store, refs.CURATED_RND_STATISTICS, check_label="unique grain")
        if m:
            return m
        p, d = check_curated_rnd_statistics_unique_grain(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_CURATED, name="output_measure_populated")
    def curated_rnd_statistics_output_measure_populated(table_store: TableStoreResource):
        df, m = read_table_for_check(table_store, refs.CURATED_RND_STATISTICS, check_label="output_measure populated")
        if m:
            return m
        p, d = check_curated_rnd_statistics_output_measure_populated(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_CURATED, name="output_value_non_negative")
    def curated_rnd_statistics_output_value_non_negative(table_store: TableStoreResource):
        df, m = read_table_for_check(table_store, refs.CURATED_RND_STATISTICS, check_label="output_value non-negative")
        if m:
            return m
        p, d = check_curated_rnd_statistics_output_value_non_negative(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_CURATED, name="provenance_columns_present")
    def curated_rnd_statistics_provenance_columns_present(table_store: TableStoreResource):
        df, m = read_table_for_check(table_store, refs.CURATED_RND_STATISTICS, check_label="provenance columns")
        if m:
            return m
        p, d = check_curated_rnd_statistics_provenance_columns_present(df)
        return AssetCheckResult(passed=p, description=d)

    @asset_check(asset=_CURATED, name="reconciles_to_site_input")
    def curated_rnd_statistics_reconciles_to_site_input(table_store: TableStoreResource):
        curated_df, m = read_table_for_check(table_store, refs.CURATED_RND_STATISTICS, check_label="curated reconciliation")
        if m:
            return m
        site_df, m2 = read_table_for_check(table_store, refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, check_label="site reconciliation")
        if m2:
            return m2
        p, d = check_curated_rnd_statistics_reconciles_to_site_input(curated_df, site_df)
        return AssetCheckResult(passed=p, description=d)

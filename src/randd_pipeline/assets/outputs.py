from __future__ import annotations

from typing import Any

from src.randd_pipeline.domain.outputs import build_curated_rnd_statistics
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetKey, Config, asset
    from pydantic import Field, field_validator
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    class CuratedRndStatisticsConfig(Config):
        measure_columns: dict[str, str] = Field(
            default={"211_apportioned": "total_211_apportioned"},
            description="Mapping from site-apportioned numeric input columns to curated output measure names.",
        )
        source_snapshot_id: str | None = Field(default=None, description="Optional source Iceberg snapshot identifier for provenance. Placeholder in local v1.")
        pipeline_run_id: str | None = Field(default=None, description="Optional pipeline run identifier for provenance. Placeholder in local v1.")

        @field_validator("measure_columns")
        @classmethod
        def _measure_columns_valid(cls, v: dict[str, str]) -> dict[str, str]:
            if not v:
                raise ValueError("measure_columns must be non-empty.")
            for source_col, measure_name in v.items():
                if not source_col.strip():
                    raise ValueError("measure_columns source column names must be non-blank.")
                if not measure_name.strip():
                    raise ValueError("measure_columns output measure names must be non-blank.")
            return v


try:
    asset
except NameError:  # pragma: no cover
    pass
else:
    @asset(key=AssetKey(["curated", "rnd_statistics"]), deps=[AssetKey(["intermediate", "site_apportioned_responses"])])
    def curated_rnd_statistics(table_store: TableStoreResource, config: CuratedRndStatisticsConfig) -> dict[str, Any]:
        store = table_store.get_table_store()
        site_df = store.read_table_as_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES)
        out = build_curated_rnd_statistics(
            site_df,
            measure_columns=config.measure_columns,
            source_table_identifier=refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES,
            source_snapshot_id=config.source_snapshot_id,
            pipeline_run_id=config.pipeline_run_id,
        )
        store.create_table_from_dataframe(refs.CURATED_RND_STATISTICS, out, overwrite=False)
        return {
            "table": refs.CURATED_RND_STATISTICS,
            "row_count": int(len(out)),
            "column_count": int(len(out.columns)),
            "output_measure_count": int(out["output_measure"].nunique(dropna=True)),
            "source_table_identifier": refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES,
        }

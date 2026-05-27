"""Manual outlier seam assets for imputed -> outlier-adjusted responses."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.randd_pipeline.domain.outliers import apply_manual_outlier_adjustments
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource


def materialise_manual_outliers(store, df, overwrite: bool = False) -> None:
    store.create_table_from_dataframe(refs.OPS_MANUAL_OUTLIERS, df, overwrite=overwrite)


def materialise_outlier_adjusted_responses(store, df, overwrite: bool = False) -> None:
    store.create_table_from_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES, df, overwrite=overwrite)


try:
    from dagster import AssetKey, Field, asset
except ModuleNotFoundError:  # pragma: no cover
    pass
else:

    @asset(
        key=AssetKey(["ops", "manual_outliers"]),
        config_schema={"manual_outliers_csv_path": str},
    )
    def manual_outliers(context, table_store: TableStoreResource) -> dict[str, Any]:
        path = context.op_config.get("manual_outliers_csv_path")
        if not path or not str(path).strip():
            raise ValueError("manual_outliers asset requires non-blank config key 'manual_outliers_csv_path'")
        store = table_store.get_table_store()
        manual = pd.read_csv(str(path))
        materialise_manual_outliers(store, manual, overwrite=False)
        return {"table": refs.OPS_MANUAL_OUTLIERS, "row_count": int(len(manual)), "column_count": int(len(manual.columns))}

    @asset(
        key=AssetKey(["intermediate", "outlier_adjusted_responses"]),
        deps=[AssetKey(["intermediate", "imputed_responses"])],
        config_schema={
            "strict_manual_references": Field(
                bool,
                default_value=True,
                description="Fail the run when manual outlier rows do not match imputed input records.",
            ),
            "default_outlier": Field(
                bool,
                default_value=False,
                description="Default outlier decision when no auto or manual outlier value is present.",
            ),
        },
    )
    def outlier_adjusted_responses(context, table_store: TableStoreResource) -> dict[str, Any]:
        store = table_store.get_table_store()
        strict_manual_references = context.op_config["strict_manual_references"]
        default_outlier = context.op_config["default_outlier"]
        imputed = store.read_table_as_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES)
        manual = store.read_table_as_dataframe(refs.OPS_MANUAL_OUTLIERS) if store.table_exists(refs.OPS_MANUAL_OUTLIERS) else None
        out = apply_manual_outlier_adjustments(
            imputed,
            manual_outliers=manual,
            strict_manual_references=strict_manual_references,
            default_outlier=default_outlier,
        )
        materialise_outlier_adjusted_responses(store, out, overwrite=False)
        return {
            "table": refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES,
            "row_count": int(len(out)),
            "column_count": int(len(out.columns)),
            "manual_rows_supplied": int(0 if manual is None else len(manual)),
            "manual_adjustments_applied": int(out["outlier_adjustment_applied"].sum()),
            "final_outlier_count": int(out["outlier"].sum()),
        }

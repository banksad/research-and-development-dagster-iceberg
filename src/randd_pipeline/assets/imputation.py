"""Minimal imputation seam asset for mapped -> imputed responses."""

from __future__ import annotations

from typing import Any

from src.randd_pipeline.domain.imputation import apply_simple_tmi_imputation
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource


def materialise_imputed_responses(store, df, overwrite: bool = False) -> None:
    store.create_table_from_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES, df, overwrite=overwrite)


try:
    from dagster import AssetKey, asset
except ModuleNotFoundError:  # pragma: no cover
    pass
else:

    @asset(key=AssetKey(["intermediate", "imputed_responses"]))
    def imputed_responses(table_store: TableStoreResource) -> dict[str, Any]:
        store = table_store.get_table_store()
        mapped = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
        imputed = apply_simple_tmi_imputation(
            mapped,
            target_column="601",
            imputation_class_column="imp_class",
            status_column="status",
            clear_statuses={"clear", "responding"},
            impute_statuses={"impute", "non_response"},
            output_column="601_imputed",
            marker_column="imp_marker",
        )
        materialise_imputed_responses(store=store, df=imputed, overwrite=False)
        return {
            "table": refs.INTERMEDIATE_IMPUTED_RESPONSES,
            "row_count": int(len(imputed)),
            "column_count": int(len(imputed.columns)),
            "imputed_count": int((imputed["imp_marker"] == "TMI").sum()),
            "no_mean_found_count": int((imputed["imp_marker"] == "no_mean_found").sum()),
        }

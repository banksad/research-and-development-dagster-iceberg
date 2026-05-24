"""Minimal imputation seam asset for mapped -> imputed responses."""

from __future__ import annotations

from typing import Any

from src.randd_pipeline.domain.imputation import apply_simple_tmi_imputation
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource


def materialise_imputed_responses(store, df, overwrite: bool = False) -> None:
    store.create_table_from_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES, df, overwrite=overwrite)


try:
    from dagster import AssetKey, Config, asset
    from pydantic import Field, field_validator
except ModuleNotFoundError:  # pragma: no cover
    pass
else:

    class SimpleTmiImputationConfig(Config):
        target_column: str = Field(
            default="601",
            description=(
                "Numeric mapped-response column to impute in the minimal v1 "
                "imputation seam."
            ),
        )
        imputation_class_column: str = Field(
            default="imp_class",
            description=(
                "Column defining the imputation class used to calculate class means."
            ),
        )
        status_column: str = Field(
            default="status",
            description=(
                "Column used to decide whether a row is clear/responding or requires "
                "imputation."
            ),
        )
        clear_statuses: list[str] = Field(
            default=["clear", "responding"],
            description=(
                "Status values treated as valid observed records for calculating "
                "class means."
            ),
        )
        impute_statuses: list[str] = Field(
            default=["impute", "non_response"],
            description="Status values treated as requiring imputation.",
        )
        output_column: str = Field(
            default="601_imputed",
            description="Output column that will contain observed or imputed values.",
        )
        marker_column: str = Field(
            default="imp_marker",
            description=(
                "Output marker column describing whether each row was not_imputed, "
                "TMI, or no_mean_found."
            ),
        )

        @field_validator(
            "target_column",
            "imputation_class_column",
            "status_column",
            "output_column",
            "marker_column",
        )
        @classmethod
        def _validate_non_blank_column_name(cls, value: str) -> str:
            if not value.strip():
                raise ValueError("Column names must be non-blank strings.")
            return value

        @field_validator("clear_statuses", "impute_statuses")
        @classmethod
        def _validate_status_list(cls, value: list[str]) -> list[str]:
            if not value:
                raise ValueError("Status lists must contain at least one value.")
            cleaned = [item.strip() for item in value]
            if any(not item for item in cleaned):
                raise ValueError("Status values must be non-blank strings.")
            if len(set(cleaned)) != len(cleaned):
                raise ValueError("Status values must be unique within each status list.")
            return cleaned

    @asset(key=AssetKey(["intermediate", "imputed_responses"]))
    def imputed_responses(table_store: TableStoreResource, config: SimpleTmiImputationConfig) -> dict[str, Any]:
        store = table_store.get_table_store()
        mapped = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
        imputed = apply_simple_tmi_imputation(
            mapped,
            target_column=config.target_column,
            imputation_class_column=config.imputation_class_column,
            status_column=config.status_column,
            clear_statuses=set(config.clear_statuses),
            impute_statuses=set(config.impute_statuses),
            output_column=config.output_column,
            marker_column=config.marker_column,
        )
        materialise_imputed_responses(store=store, df=imputed, overwrite=False)
        return {
            "table": refs.INTERMEDIATE_IMPUTED_RESPONSES,
            "row_count": int(len(imputed)),
            "column_count": int(len(imputed.columns)),
            "imputed_count": int((imputed[config.marker_column] == "TMI").sum()),
            "no_mean_found_count": int((imputed[config.marker_column] == "no_mean_found").sum()),
        }

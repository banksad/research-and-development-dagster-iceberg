from typing import Any, List

from src.randd_pipeline.domain.estimation import calculate_minimal_estimation_weights
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetKey, Config, asset
    from pydantic import Field, field_validator
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    class MinimalEstimationWeightsConfig(Config):
        cell_column: str = Field(default="cellnumber", description="Column identifying the estimation cell.")
        population_count_column: str = Field(default="uni_count", description="Column containing the population count for the estimation cell.")
        population_employment_column: str = Field(default="uni_employment", description="Column containing population employment for the estimation cell.")
        employment_column: str = Field(default="employment", description="Column containing response employment used in g-weight calculation.")
        outlier_column: str = Field(default="outlier", description="Column containing the final outlier decision.")
        selection_type_column: str = Field(default="selectiontype", description="Column used to identify sampled PRN rows.")
        form_type_column: str = Field(default="formtype", description="Column used to identify short-form rows.")
        status_column: str = Field(default="status", description="Column used to identify clear/valid records.")
        target_column: str = Field(default="709", description="Column required to be present for records used in estimation.")
        selected_selection_type: str = Field(default="P", description="Selection type included in the minimal v1 weighting calculation.")
        selected_form_type: str = Field(default="0006", description="Form type included in the minimal v1 weighting calculation.")
        clear_statuses: List[str] = Field(default_factory=lambda: ["Clear", "Clear - overridden"], description="Statuses treated as clear/valid for estimation.")
        selected_instance: int = Field(default=0, description="Response instance included in the minimal v1 weighting calculation.")

        @field_validator("cell_column", "population_count_column", "population_employment_column", "employment_column", "outlier_column", "selection_type_column", "form_type_column", "status_column", "target_column", "selected_selection_type", "selected_form_type")
        @classmethod
        def _non_blank(cls, value: str) -> str:
            if not value.strip():
                raise ValueError("Config string fields must be non-blank.")
            return value

        @field_validator("clear_statuses")
        @classmethod
        def _clear_statuses_valid(cls, value: List[str]) -> List[str]:
            if not value or any(not v.strip() for v in value):
                raise ValueError("clear_statuses must be non-empty and contain no blank values.")
            return value

    @asset(
        key=AssetKey(["intermediate", "estimated_responses"]),
        deps=[AssetKey(["intermediate", "outlier_adjusted_responses"])],
    )
    def estimated_responses(table_store: TableStoreResource, config: MinimalEstimationWeightsConfig) -> dict[str, Any]:
        store = table_store.get_table_store()
        outlier_adjusted = store.read_table_as_dataframe(refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)
        estimated = calculate_minimal_estimation_weights(
            outlier_adjusted,
            cell_column=config.cell_column,
            population_count_column=config.population_count_column,
            population_employment_column=config.population_employment_column,
            employment_column=config.employment_column,
            outlier_column=config.outlier_column,
            selection_type_column=config.selection_type_column,
            form_type_column=config.form_type_column,
            status_column=config.status_column,
            target_column=config.target_column,
            selected_selection_type=config.selected_selection_type,
            selected_form_type=config.selected_form_type,
            clear_statuses=tuple(config.clear_statuses),
            selected_instance=config.selected_instance,
        )
        store.create_table_from_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES, estimated, overwrite=False)
        weighted = int(((estimated["a_weight"] != 1.0) | (estimated["g_weight"] != 1.0)).sum())
        outliers = int(estimated[config.outlier_column].fillna(False).astype(bool).sum())
        return {"table": refs.INTERMEDIATE_ESTIMATED_RESPONSES, "row_count": int(len(estimated)), "column_count": int(len(estimated.columns)), "weighted_row_count": weighted, "outlier_row_count": outliers}

from __future__ import annotations

import pandas as pd


GRAIN_COLUMNS = ["reference", "instance", "survey_type", "survey_year"]


def _validate_required_columns(df: pd.DataFrame, required: set[str], df_name: str) -> None:
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {', '.join(missing)}")


def apply_foreign_ownership_mapping(
    staged_responses: pd.DataFrame,
    ultfoc_mapper: pd.DataFrame,
    *,
    response_reference_column: str = "reference",
    mapper_reference_column: str = "ruref",
    ownership_column: str = "ultfoc",
    default_ownership: str = "GB",
) -> pd.DataFrame:
    """Apply GB-side foreign ownership mapping and return a canonical `ultfoc` output column.

    This function intentionally overwrites any existing ownership values in
    `staged_responses[ownership_column]` with values resolved from the provided mapper,
    defaulting to `default_ownership` for missing/blank/null mapper values.
    """

    _validate_required_columns(
        staged_responses,
        required={response_reference_column, *GRAIN_COLUMNS},
        df_name="staged_responses",
    )
    _validate_required_columns(
        ultfoc_mapper,
        required={mapper_reference_column, ownership_column},
        df_name="ultfoc_mapper",
    )

    responses = staged_responses.copy(deep=True)
    mapper = ultfoc_mapper[[mapper_reference_column, ownership_column]].copy(deep=True)

    mapper_ownership_column = f"{ownership_column}_mapped"
    mapper = mapper.rename(columns={ownership_column: mapper_ownership_column})

    mapped = responses.merge(
        mapper,
        how="left",
        left_on=response_reference_column,
        right_on=mapper_reference_column,
    )

    resolved = mapped[mapper_ownership_column].where(
        mapped[mapper_ownership_column].notna()
        & mapped[mapper_ownership_column].astype(str).str.strip().ne(""),
        default_ownership,
    )
    mapped[ownership_column] = resolved

    if mapper_reference_column in mapped.columns:
        mapped = mapped.drop(columns=[mapper_reference_column])
    mapped = mapped.drop(columns=[mapper_ownership_column])

    return mapped

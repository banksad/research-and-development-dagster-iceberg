"""Minimal deterministic class-mean imputation seam (simple TMI-style)."""

from __future__ import annotations

import pandas as pd
from pandas.api.types import is_numeric_dtype

_REQUIRED_BASE_COLUMNS = ["reference", "instance", "survey_type", "survey_year"]


def apply_simple_tmi_imputation(
    mapped_responses: pd.DataFrame,
    *,
    target_column: str,
    imputation_class_column: str,
    status_column: str,
    clear_statuses: set[str] | list[str],
    impute_statuses: set[str] | list[str],
    output_column: str | None = None,
    marker_column: str = "imp_marker",
) -> pd.DataFrame:
    """Apply minimal class-mean imputation without mutating input.

    Behaviour when no class mean exists: leave ``output_column`` as original value
    (often null for records needing imputation) and set marker to ``no_mean_found``.
    """

    out_col = output_column or f"{target_column}_imputed"
    required = [
        *_REQUIRED_BASE_COLUMNS,
        target_column,
        imputation_class_column,
        status_column,
    ]
    missing = [col for col in required if col not in mapped_responses.columns]
    if missing:
        raise ValueError(f"Missing required columns for simple imputation: {', '.join(missing)}")

    result = mapped_responses.copy(deep=True)

    target_series = result[target_column]
    non_null_target = target_series[target_series.notna()]
    if not non_null_target.empty and not is_numeric_dtype(non_null_target):
        raise ValueError(
            f"Column '{target_column}' must be numeric for class-mean imputation. "
            "Found non-numeric values."
        )

    clear_mask = result[status_column].isin(set(clear_statuses))
    impute_mask = result[status_column].isin(set(impute_statuses))

    class_means = (
        result.loc[clear_mask & result[target_column].notna()]
        .groupby(imputation_class_column, dropna=False)[target_column]
        .mean()
    )

    result[out_col] = result[target_column]
    result[marker_column] = "not_imputed"

    needs_imputation = impute_mask
    class_mean_values = result.loc[needs_imputation, imputation_class_column].map(class_means)

    has_mean = class_mean_values.notna()
    idx_with_mean = class_mean_values.index[has_mean]
    idx_without_mean = class_mean_values.index[~has_mean]

    result.loc[idx_with_mean, out_col] = class_mean_values.loc[idx_with_mean]
    result.loc[idx_with_mean, marker_column] = "TMI"
    result.loc[idx_without_mean, marker_column] = "no_mean_found"

    return result

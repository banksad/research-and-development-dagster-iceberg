from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd


_DEFAULT_MEASURE_COLUMNS: dict[str, str] = {"211_apportioned": "total_211_apportioned"}


def build_curated_rnd_statistics(
    site_apportioned_responses: pd.DataFrame,
    *,
    measure_columns: Mapping[str, str] | None = None,
    group_columns: Sequence[str] = ("survey_year", "survey_type"),
    source_table_identifier: str = "intermediate.site_apportioned_responses",
    source_snapshot_id: str | None = None,
    pipeline_run_id: str | None = None,
) -> pd.DataFrame:
    if site_apportioned_responses.empty:
        raise ValueError("site_apportioned_responses must be non-empty.")

    measures = dict(_DEFAULT_MEASURE_COLUMNS if measure_columns is None else measure_columns)
    if not measures:
        raise ValueError("measure_columns must be non-empty.")

    for src, out in measures.items():
        if not str(src).strip():
            raise ValueError("measure_columns cannot include blank source column names.")
        if not str(out).strip():
            raise ValueError("measure_columns cannot include blank output measure names.")

    missing_groups = [c for c in group_columns if c not in site_apportioned_responses.columns]
    if missing_groups:
        raise ValueError(f"Missing required group column(s): {', '.join(missing_groups)}")

    missing_measures = [c for c in measures if c not in site_apportioned_responses.columns]
    if missing_measures:
        raise ValueError(f"Missing required measure column(s): {', '.join(missing_measures)}")

    working = site_apportioned_responses.copy(deep=True)
    for col in measures:
        numeric = pd.to_numeric(working[col], errors="coerce")
        if int(numeric.isna().sum()):
            raise ValueError(f"Measure column '{col}' contains non-numeric or null values.")
        working[col] = numeric

    grouped = working.groupby(list(group_columns), dropna=False)[list(measures.keys())].sum().reset_index()
    long_df = grouped.melt(id_vars=list(group_columns), var_name="_source_measure_column", value_name="output_value")
    long_df["output_measure"] = long_df["_source_measure_column"].map(measures)
    long_df["source_table_identifier"] = source_table_identifier
    long_df["source_snapshot_id"] = source_snapshot_id
    long_df["pipeline_run_id"] = pipeline_run_id
    return long_df[list(group_columns) + ["output_measure", "output_value", "source_table_identifier", "source_snapshot_id", "pipeline_run_id"]]

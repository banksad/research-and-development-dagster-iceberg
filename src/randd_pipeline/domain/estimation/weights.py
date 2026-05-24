from __future__ import annotations

import pandas as pd


_GRAIN = ["reference", "instance", "survey_type", "survey_year"]


def calculate_minimal_estimation_weights(
    outlier_adjusted_responses: pd.DataFrame,
    *,
    cell_column: str = "cellnumber",
    population_count_column: str = "uni_count",
    population_employment_column: str = "uni_employment",
    employment_column: str = "employment",
    outlier_column: str = "outlier",
    selection_type_column: str = "selectiontype",
    form_type_column: str = "formtype",
    status_column: str = "status",
    instance_column: str = "instance",
    reference_column: str = "reference",
    target_column: str = "709",
    selected_selection_type: str = "P",
    selected_form_type: str = "0006",
    clear_statuses: tuple[str, ...] = ("Clear", "Clear - overridden"),
    selected_instance: int = 0,
) -> pd.DataFrame:
    required = set(_GRAIN + [cell_column, population_count_column, population_employment_column, employment_column, outlier_column, selection_type_column, form_type_column, status_column, instance_column, reference_column, target_column])
    missing = sorted(c for c in required if c not in outlier_adjusted_responses.columns)
    if missing:
        raise ValueError(f"Missing required columns for minimal estimation weighting: {', '.join(missing)}")

    out = outlier_adjusted_responses.copy(deep=True)
    for col in [population_count_column, population_employment_column, employment_column]:
        out[col] = pd.to_numeric(out[col], errors="raise")
    mask_target = out[target_column].notna()
    if mask_target.any():
        out.loc[mask_target, target_column] = pd.to_numeric(out.loc[mask_target, target_column], errors="raise")

    out["a_weight"] = 1.0
    out["g_weight"] = 1.0

    weight_eligible = (out[selection_type_column] == selected_selection_type) & (out[form_type_column] == selected_form_type)
    estimation_eligible = weight_eligible & out[status_column].isin(clear_statuses) & (out[instance_column] == selected_instance) & out[target_column].notna()

    for cell, idx in out.groupby(cell_column).groups.items():
        idx = list(idx)
        cell_est = out.loc[idx][estimation_eligible.loc[idx]]
        if cell_est.empty:
            continue
        n = int(cell_est[reference_column].nunique())
        o = int(cell_est[outlier_column].fillna(False).astype(bool).sum())
        N = float(cell_est[population_count_column].iloc[0])
        a_weight = (N - o) / (n - o) if (n - o) > 0 else 1.0

        E = float(cell_est[population_employment_column].iloc[0])
        e = float(cell_est[employment_column].sum())
        s = float(cell_est.loc[cell_est[outlier_column].fillna(False).astype(bool), employment_column].sum())
        denom = a_weight * (e - s)
        g_weight = (E - s) / denom if denom > 0 else 1.0

        apply_mask = out.index.isin(idx) & weight_eligible
        out.loc[apply_mask, "a_weight"] = float(a_weight)
        out.loc[apply_mask, "g_weight"] = float(g_weight)

    outlier_mask = out[outlier_column].fillna(False).astype(bool)
    out.loc[outlier_mask, ["a_weight", "g_weight"]] = 1.0
    return out

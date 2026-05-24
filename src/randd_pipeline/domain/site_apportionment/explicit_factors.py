from __future__ import annotations

import pandas as pd

_RESPONSE_GRAIN = ["reference", "instance", "survey_type", "survey_year"]
_FACTOR_REQUIRED = [*_RESPONSE_GRAIN, "site_id", "site_proportion"]


def _missing(df: pd.DataFrame, cols: list[str]) -> list[str]:
    return [c for c in cols if c not in df.columns]


def apply_explicit_site_apportionment(estimated_responses: pd.DataFrame, site_factors: pd.DataFrame, *, value_columns: tuple[str, ...] | list[str], output_suffix: str = "_apportioned", strict_factor_coverage: bool = True, factor_sum_tolerance: float = 1e-9) -> pd.DataFrame:
    if not value_columns:
        raise ValueError("value_columns must be non-empty.")
    if not output_suffix.strip():
        raise ValueError("output_suffix must be non-blank.")
    if factor_sum_tolerance <= 0:
        raise ValueError("factor_sum_tolerance must be positive.")
    miss = _missing(estimated_responses, _RESPONSE_GRAIN)
    if miss: raise ValueError(f"estimated_responses missing required columns: {miss}")
    miss = _missing(site_factors, _FACTOR_REQUIRED)
    if miss: raise ValueError(f"site_factors missing required columns: {miss}")
    for c in value_columns:
        if c not in estimated_responses.columns: raise ValueError(f"Requested value column missing: {c}")
        if pd.to_numeric(estimated_responses[c], errors="coerce").isna().any(): raise ValueError(f"Value column must be numeric: {c}")
    props = pd.to_numeric(site_factors["site_proportion"], errors="coerce")
    if props.isna().any(): raise ValueError("site_proportion must be numeric.")
    if (props < 0).any(): raise ValueError("site_proportion must be non-negative.")
    fac = site_factors.copy(); fac["site_proportion"] = props
    if fac.duplicated(subset=[*_RESPONSE_GRAIN, "site_id"], keep=False).any(): raise ValueError("Duplicate factor rows found at response+site grain.")
    est_keys = estimated_responses[_RESPONSE_GRAIN].drop_duplicates()
    fac_keys = fac[_RESPONSE_GRAIN].drop_duplicates()
    extra = fac_keys.merge(est_keys, on=_RESPONSE_GRAIN, how="left", indicator=True)
    if (extra["_merge"]=="left_only").any(): raise ValueError("site_factors contains rows for responses missing from estimated_responses.")
    sums = fac.groupby(_RESPONSE_GRAIN, dropna=False)["site_proportion"].sum().reset_index()
    if ((sums["site_proportion"] - 1.0).abs() > factor_sum_tolerance).any(): raise ValueError("site_proportion must sum to 1.0 per response grain within tolerance.")
    merged = estimated_responses.copy().merge(fac, on=_RESPONSE_GRAIN, how="left", indicator=True)
    if strict_factor_coverage and (merged["_merge"]=="left_only").any(): raise ValueError("Estimated responses missing site factors for one or more rows.")
    merged = merged[merged["_merge"]=="both"].drop(columns=["_merge"])
    for c in value_columns:
        merged[f"{c}{output_suffix}"] = pd.to_numeric(merged[c], errors="coerce") * merged["site_proportion"]
    merged["apportionment_method"] = "explicit_site_factor"
    merged["factor_source"] = "ref.site_apportionment_factors"
    return merged

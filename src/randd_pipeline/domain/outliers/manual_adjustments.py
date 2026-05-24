from __future__ import annotations

import pandas as pd

_GRAIN = ["reference", "instance", "survey_type", "survey_year"]
_MANUAL_REQUIRED = [*_GRAIN, "manual_outlier", "outlier_reason"]


def _normalise_manual_outlier(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned == "true":
            return True
        if cleaned == "false":
            return False
    raise ValueError(f"manual_outlier must be boolean-like True/False; got {value!r}")


def apply_manual_outlier_adjustments(
    imputed_responses: pd.DataFrame,
    manual_outliers: pd.DataFrame | None = None,
    *,
    strict_manual_references: bool = True,
    default_outlier: bool = False,
) -> pd.DataFrame:
    missing = [col for col in _GRAIN if col not in imputed_responses.columns]
    if missing:
        raise ValueError(f"imputed_responses missing required grain columns: {', '.join(missing)}")

    base = imputed_responses.copy(deep=True)
    if "auto_outlier" in base.columns:
        auto_present = base["auto_outlier"].notna()
        base["outlier"] = base["auto_outlier"].fillna(default_outlier).astype(bool)
        base["outlier_source"] = auto_present.map({True: "auto", False: "default_none"})
    else:
        base["outlier"] = bool(default_outlier)
        base["outlier_source"] = "default_none"
    base["outlier_adjustment_applied"] = False
    base["outlier_reason"] = None

    if manual_outliers is None or manual_outliers.empty:
        return base

    manual = manual_outliers.copy(deep=True)
    missing_manual = [col for col in _MANUAL_REQUIRED if col not in manual.columns]
    if missing_manual:
        raise ValueError(f"manual_outliers missing required columns: {', '.join(missing_manual)}")

    dupes = int(manual.duplicated(subset=_GRAIN, keep=False).sum())
    if dupes:
        raise ValueError(f"manual_outliers contains duplicate response grain rows: {dupes}")

    manual["manual_outlier"] = manual["manual_outlier"].map(_normalise_manual_outlier)
    blank_reason = manual["outlier_reason"].isna() | manual["outlier_reason"].astype(str).str.strip().eq("")
    if int(blank_reason.sum()):
        raise ValueError("manual_outliers outlier_reason must be populated when manual_outlier is supplied")

    unmatched = int(manual.merge(base[_GRAIN].drop_duplicates(), on=_GRAIN, how="left", indicator=True)["_merge"].eq("left_only").sum())
    merged = base.merge(manual[_MANUAL_REQUIRED], on=_GRAIN, how="left")
    if strict_manual_references and unmatched:
        raise ValueError(f"manual_outliers has {unmatched} row(s) not present in imputed_responses")

    has_manual = merged["manual_outlier"].notna()
    merged.loc[has_manual, "outlier"] = merged.loc[has_manual, "manual_outlier"].astype(bool)
    merged.loc[has_manual, "outlier_source"] = "manual_outlier"
    merged.loc[has_manual, "outlier_adjustment_applied"] = True
    merged.loc[has_manual, "outlier_reason"] = merged.loc[has_manual, "outlier_reason_y"]

    result = merged.drop(columns=["manual_outlier", "outlier_reason_y"])
    result = result.rename(columns={"outlier_reason_x": "outlier_reason"})
    return result

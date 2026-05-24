from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


def check_required_columns_present(
    df: pd.DataFrame, required_columns: Iterable[str], *, label: str = "dataframe"
) -> tuple[bool, str]:
    required = list(required_columns)
    missing = [c for c in required if c not in df.columns]
    if missing:
        return False, f"Cannot validate {label}; missing columns: {', '.join(missing)}"
    return True, f"{label} includes all required columns ({len(required)} column(s) required)."


def check_unique_grain(df: pd.DataFrame, grain_columns: Iterable[str], *, grain_name: str = "grain") -> tuple[bool, str]:
    cols = list(grain_columns)
    missing = [c for c in cols if c not in df.columns]
    if missing:
        return False, f"Cannot validate {grain_name}; missing columns: {', '.join(missing)}"
    dupes = int(df.duplicated(subset=cols, keep=False).sum())
    if dupes:
        return False, f"Found duplicate rows at {grain_name}: {dupes} duplicate row(s)."
    return True, f"{grain_name} is unique ({len(df)} row(s) checked)."


def check_column_non_null_non_blank(df: pd.DataFrame, column: str, *, label: str | None = None) -> tuple[bool, str]:
    name = label or column
    if column not in df.columns:
        return False, f"Missing {column} column."
    series = df[column]
    nulls = int(series.isna().sum())
    blanks = int(series.astype(str).str.strip().eq("").sum())
    bad = nulls + blanks
    if bad:
        return False, f"{name} contains {bad} null/blank value(s)."
    return True, f"{name} populated for all {len(df)} row(s)."


def check_numeric_column_populated(df: pd.DataFrame, column: str, *, label: str | None = None) -> tuple[bool, str]:
    name = label or column
    if column not in df.columns:
        return False, f"Missing {column} column."
    values = pd.to_numeric(df[column], errors="coerce")
    bad = int(values.isna().sum())
    if bad:
        return False, f"{name} contains null or non-numeric values."
    return True, f"{name} populated with numeric values for all {len(df)} row(s)."


def check_numeric_column_non_negative(df: pd.DataFrame, column: str, *, label: str | None = None) -> tuple[bool, str]:
    name = label or column
    populated_ok, msg = check_numeric_column_populated(df, column, label=name)
    if not populated_ok:
        return False, msg
    values = pd.to_numeric(df[column], errors="coerce")
    if int((values < 0).sum()):
        return False, f"{name} contains negative values."
    return True, f"{name} populated and non-negative for all {len(df)} row(s)."


def check_numeric_column_positive(df: pd.DataFrame, column: str, *, label: str | None = None) -> tuple[bool, str]:
    name = label or column
    populated_ok, msg = check_numeric_column_populated(df, column, label=name)
    if not populated_ok:
        return False, msg
    values = pd.to_numeric(df[column], errors="coerce")
    non_positive = int((values <= 0).sum())
    if non_positive:
        return False, f"{name} must be strictly positive; found {non_positive} non-positive value(s)."
    return True, f"{name} is strictly positive numeric for all {len(df)} row(s)."


def check_allowed_values(
    df: pd.DataFrame, column: str, allowed_values: Iterable[object], *, label: str | None = None
) -> tuple[bool, str]:
    name = label or column
    if column not in df.columns:
        return False, f"Missing {column} column."
    allowed = set(allowed_values)
    invalid = df[~df[column].isin(allowed)][column].dropna().unique().tolist()
    if invalid:
        return False, f"{name} contains disallowed values: {', '.join(map(str, invalid[:5]))}"
    return True, f"{name} only contains allowed values ({len(allowed)} allowed value(s))."


def check_group_sum_close_to(
    df: pd.DataFrame,
    group_columns: Iterable[str],
    value_column: str,
    *,
    expected: float = 1.0,
    tolerance: float = 1e-9,
    label: str = "group sum",
) -> tuple[bool, str]:
    group_cols = list(group_columns)
    cols = [*group_cols, value_column]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        return False, f"Cannot validate {label}; missing columns: {', '.join(missing)}"
    values = pd.to_numeric(df[value_column], errors="coerce")
    if int(values.isna().sum()):
        return False, f"{value_column} contains null or non-numeric values."
    grouped = values.groupby([df[c] for c in group_cols], dropna=False).sum()
    bad = int(((grouped - expected).abs() > tolerance).sum())
    if bad:
        return False, f"Found {bad} group(s) where {label} does not equal {expected}."
    return True, f"{label} equals {expected} across {int(grouped.shape[0])} group(s)."

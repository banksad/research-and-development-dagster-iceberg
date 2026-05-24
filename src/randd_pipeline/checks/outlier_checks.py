from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"
_GRAIN = ["reference", "instance", "survey_type", "survey_year"]
_ALLOWED_SOURCES = {"manual_outlier", "auto", "default_none"}


def _outlier_contract():
    contracts = load_table_contracts(_CONTRACTS_PATH)
    return get_table_contract(contracts, refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES)


def check_outlier_adjusted_required_columns(df: pd.DataFrame): return check_contract_required_columns(df, _outlier_contract())
def check_outlier_adjusted_non_empty(df: pd.DataFrame): return check_non_empty(df)

def check_outlier_adjusted_unique_grain(df: pd.DataFrame):
    missing = [c for c in _GRAIN if c not in df.columns]
    if missing: return False, f"Cannot validate grain uniqueness; missing columns: {', '.join(missing)}"
    dupes = int(df.duplicated(subset=_GRAIN, keep=False).sum())
    return (False, f"Found duplicate rows at response grain: {dupes} duplicate row(s).") if dupes else (True, f"Response grain is unique at {'+'.join(_GRAIN)} ({len(df)} row(s) checked).")

def check_outlier_flag_populated(df: pd.DataFrame):
    if "outlier" not in df.columns: return False, "Cannot validate outlier population; missing column: outlier"
    bad = int(df["outlier"].isna().sum())
    return (False, f"outlier must be populated for all rows; found {bad} null value(s).") if bad else (True, f"outlier is populated for all {len(df)} row(s).")

def check_outlier_source_populated(df: pd.DataFrame):
    if "outlier_source" not in df.columns: return False, "Cannot validate outlier_source population; missing column: outlier_source"
    s = df["outlier_source"].astype(str).str.strip()
    blanks = int(df["outlier_source"].isna().sum() + s.eq("").sum())
    invalid = int((~s.isin(_ALLOWED_SOURCES) & ~s.eq("")).sum())
    if blanks: return False, f"outlier_source must be populated for all rows; found {blanks} blank/null value(s)."
    if invalid: return False, f"outlier_source contains {invalid} invalid value(s); allowed values: {sorted(_ALLOWED_SOURCES)}"
    return True, f"outlier_source populated with allowed values for all {len(df)} row(s)."

def check_manual_adjustment_reason_present(df: pd.DataFrame):
    needed = {"outlier_adjustment_applied", "outlier_reason"}
    if any(c not in df.columns for c in needed): return False, "Cannot validate manual reason population; requires columns: outlier_adjustment_applied, outlier_reason"
    mask = df["outlier_adjustment_applied"].fillna(False).astype(bool)
    bad = int((df.loc[mask, "outlier_reason"].isna() | df.loc[mask, "outlier_reason"].astype(str).str.strip().eq("")).sum())
    return (False, f"outlier_reason must be populated when outlier_adjustment_applied is true; found {bad} invalid row(s).") if bad else (True, f"outlier_reason populated for all {int(mask.sum())} manual-adjusted row(s).")

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"
_GRAIN = ["reference", "instance", "survey_type", "survey_year"]


def _estimated_contract():
    contracts = load_table_contracts(_CONTRACTS_PATH)
    return get_table_contract(contracts, refs.INTERMEDIATE_ESTIMATED_RESPONSES)


def check_estimated_responses_required_columns(df: pd.DataFrame): return check_contract_required_columns(df, _estimated_contract())
def check_estimated_responses_non_empty(df: pd.DataFrame): return check_non_empty(df)

def check_estimated_responses_unique_grain(df: pd.DataFrame):
    missing = [c for c in _GRAIN if c not in df.columns]
    if missing: return False, f"Cannot validate grain uniqueness; missing columns: {', '.join(missing)}"
    dupes = int(df.duplicated(subset=_GRAIN, keep=False).sum())
    return (False, f"Found duplicate rows at response grain: {dupes} duplicate row(s).") if dupes else (True, f"Response grain is unique at {'+'.join(_GRAIN)} ({len(df)} row(s) checked).")

def check_estimation_weights_populated(df: pd.DataFrame):
    for col in ["a_weight", "g_weight"]:
        if col not in df.columns: return False, f"Cannot validate weight population; missing column: {col}"
        if int(df[col].isna().sum()): return False, f"{col} must be populated for all rows; found {int(df[col].isna().sum())} null value(s)."
    return True, f"a_weight and g_weight populated for all {len(df)} row(s)."

def check_estimation_weights_positive(df: pd.DataFrame):
    for col in ["a_weight", "g_weight"]:
        if col not in df.columns: return False, f"Cannot validate positive weights; missing column: {col}"
        vals = pd.to_numeric(df[col], errors="coerce")
        if int(vals.isna().sum()): return False, f"{col} contains non-numeric values."
        if int(np.isinf(vals).sum()): return False, f"{col} contains infinite values."
        if int((vals <= 0).sum()): return False, f"{col} must be strictly positive; found {int((vals <= 0).sum())} non-positive value(s)."
    return True, f"a_weight and g_weight are positive finite numeric values for all {len(df)} row(s)."

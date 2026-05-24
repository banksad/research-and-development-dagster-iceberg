from __future__ import annotations

from pathlib import Path
import pandas as pd
from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"
_RESPONSE_SITE_GRAIN = ["reference", "instance", "survey_type", "survey_year", "site_id"]
_RESPONSE_GRAIN = ["reference", "instance", "survey_type", "survey_year"]

def _contract(identifier: str):
    return get_table_contract(load_table_contracts(_CONTRACTS_PATH), identifier)

def check_site_apportioned_required_columns(df: pd.DataFrame): return check_contract_required_columns(df, _contract(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES))
def check_site_apportioned_non_empty(df: pd.DataFrame): return check_non_empty(df)
def check_site_factors_required_columns(df: pd.DataFrame): return check_contract_required_columns(df, _contract(refs.REF_SITE_APPORTIONMENT_FACTORS))

def _unique(df: pd.DataFrame, cols: list[str], name: str):
    m=[c for c in cols if c not in df.columns]
    if m: return False, f"Cannot validate {name}; missing columns: {', '.join(m)}"
    d=int(df.duplicated(subset=cols, keep=False).sum())
    return (False, f"Found duplicate rows at {name}: {d} duplicate row(s).") if d else (True, f"{name} is unique ({len(df)} row(s) checked).")

def check_site_apportioned_unique_site_grain(df: pd.DataFrame): return _unique(df, _RESPONSE_SITE_GRAIN, "response+site grain")
def check_site_factors_unique_site_grain(df: pd.DataFrame): return _unique(df, _RESPONSE_SITE_GRAIN, "factor response+site grain")

def check_site_identifier_populated(df: pd.DataFrame):
    if "site_id" not in df.columns: return False, "Missing site_id column."
    n=int(df["site_id"].isna().sum() + (df["site_id"].astype(str).str.strip()=="").sum())
    return (False, f"site_id contains {n} null/blank value(s).") if n else (True, f"site_id populated for all {len(df)} row(s).")

def check_site_proportion_populated(df: pd.DataFrame):
    if "site_proportion" not in df.columns: return False, "Missing site_proportion column."
    vals=pd.to_numeric(df["site_proportion"], errors='coerce')
    if int(vals.isna().sum()): return False, "site_proportion contains null or non-numeric values."
    if int((vals<0).sum()): return False, "site_proportion contains negative values."
    return True, f"site_proportion populated and non-negative for all {len(df)} row(s)."

def check_apportioned_values_non_negative(df: pd.DataFrame, column: str = "211_apportioned"):
    if column not in df.columns: return False, f"Missing apportioned column: {column}"
    vals=pd.to_numeric(df[column], errors='coerce')
    if int(vals.isna().sum()): return False, f"{column} contains null/non-numeric values."
    if int((vals<0).sum()): return False, f"{column} contains negative values."
    return True, f"{column} is non-negative numeric for all {len(df)} row(s)."

def check_site_factors_proportions_sum_to_one(df: pd.DataFrame, tolerance: float = 1e-9):
    m=[c for c in [*_RESPONSE_GRAIN,'site_proportion'] if c not in df.columns]
    if m: return False, f"Cannot validate factor proportion sums; missing columns: {', '.join(m)}"
    vals = pd.to_numeric(df['site_proportion'], errors='coerce')
    if int(vals.isna().sum()):
        return False, 'site_proportion contains null or non-numeric values.'
    s = vals.groupby([df[c] for c in _RESPONSE_GRAIN], dropna=False).sum()
    bad=int(((s-1.0).abs()>tolerance).sum())
    return (False, f"Found {bad} response grain(s) where site proportions do not sum to 1.0.") if bad else (True, f"Site proportions sum to 1.0 across {int(s.shape[0])} response grain(s).")

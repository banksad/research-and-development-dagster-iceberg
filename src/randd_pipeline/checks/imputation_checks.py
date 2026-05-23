from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"
_GRAIN = ["reference", "instance", "survey_type", "survey_year"]


def _imputed_contract():
    contracts = load_table_contracts(_CONTRACTS_PATH)
    return get_table_contract(contracts, refs.INTERMEDIATE_IMPUTED_RESPONSES)


def check_imputed_responses_required_columns(df: pd.DataFrame) -> tuple[bool, str]:
    return check_contract_required_columns(df, _imputed_contract())


def check_imputed_responses_non_empty(df: pd.DataFrame) -> tuple[bool, str]:
    return check_non_empty(df)


def check_imputed_responses_unique_grain(df: pd.DataFrame) -> tuple[bool, str]:
    missing = [c for c in _GRAIN if c not in df.columns]
    if missing:
        return False, f"Cannot validate grain uniqueness; missing columns: {', '.join(missing)}"
    dupes = int(df.duplicated(subset=_GRAIN, keep=False).sum())
    return (False, f"Found duplicate rows at response grain: {dupes} duplicate row(s).") if dupes else (True, f"Response grain is unique at {'+'.join(_GRAIN)} ({len(df)} row(s) checked).")


def check_imputation_marker_populated(df: pd.DataFrame) -> tuple[bool, str]:
    if "imp_marker" not in df.columns:
        return False, "Cannot validate marker population; missing column: imp_marker"
    blank_or_null = int(df["imp_marker"].isna().sum() + df["imp_marker"].astype(str).str.strip().eq("").sum())
    return (False, f"imp_marker must be populated for all rows; found {blank_or_null} empty marker(s).") if blank_or_null else (True, f"imp_marker is populated for all {len(df)} row(s).")


def check_no_illegal_missing_imputed_values(df: pd.DataFrame) -> tuple[bool, str]:
    if "601_imputed" not in df.columns or "imp_marker" not in df.columns:
        return False, "Cannot validate imputed-value completeness; requires columns: 601_imputed, imp_marker"
    allowed_missing = df["imp_marker"].eq("no_mean_found")
    illegal_missing = int((df["601_imputed"].isna() & ~allowed_missing).sum())
    no_mean_count = int(allowed_missing.sum())
    if illegal_missing:
        return False, f"Found {illegal_missing} illegal missing 601_imputed value(s). Rows marked no_mean_found={no_mean_count} are allowed to remain missing."
    return True, f"No illegal missing 601_imputed values. no_mean_found rows={no_mean_count}."

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.randd_pipeline.checks.common import (
    check_column_non_null_non_blank,
    check_group_sum_close_to,
    check_numeric_column_non_negative,
    check_required_columns_present,
    check_unique_grain,
)
from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"
_RESPONSE_SITE_GRAIN = ["reference", "instance", "survey_type", "survey_year", "site_id"]
_RESPONSE_GRAIN = ["reference", "instance", "survey_type", "survey_year"]


def _contract(identifier: str):
    return get_table_contract(load_table_contracts(_CONTRACTS_PATH), identifier)


def check_site_apportioned_required_columns(df: pd.DataFrame):
    return check_contract_required_columns(df, _contract(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES))


def check_site_apportioned_non_empty(df: pd.DataFrame):
    return check_non_empty(df)


def check_site_factors_required_columns(df: pd.DataFrame):
    return check_contract_required_columns(df, _contract(refs.REF_SITE_APPORTIONMENT_FACTORS))


def check_site_apportioned_unique_site_grain(df: pd.DataFrame):
    return check_unique_grain(df, _RESPONSE_SITE_GRAIN, grain_name="response+site grain")


def check_site_factors_unique_site_grain(df: pd.DataFrame):
    return check_unique_grain(df, _RESPONSE_SITE_GRAIN, grain_name="factor response+site grain")


def check_site_identifier_populated(df: pd.DataFrame):
    return check_column_non_null_non_blank(df, "site_id", label="site_id")


def check_site_proportion_populated(df: pd.DataFrame):
    return check_numeric_column_non_negative(df, "site_proportion", label="site_proportion")


def check_apportioned_values_non_negative(df: pd.DataFrame, column: str = "211_apportioned"):
    required_ok, msg = check_required_columns_present(df, [column], label="apportioned values")
    if not required_ok:
        return False, f"Missing apportioned column: {column}"
    return check_numeric_column_non_negative(df, column, label=column)


def check_site_factors_proportions_sum_to_one(df: pd.DataFrame, tolerance: float = 1e-9):
    return check_group_sum_close_to(
        df,
        _RESPONSE_GRAIN,
        "site_proportion",
        expected=1.0,
        tolerance=tolerance,
        label="factor proportion sums",
    )

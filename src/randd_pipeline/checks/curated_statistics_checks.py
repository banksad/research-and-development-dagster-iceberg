from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.randd_pipeline.checks.common import (
    check_column_non_null_non_blank,
    check_numeric_column_non_negative,
    check_required_columns_present,
    check_unique_grain,
)
from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"


def _contract(identifier: str):
    return get_table_contract(load_table_contracts(_CONTRACTS_PATH), identifier)


def check_curated_rnd_statistics_required_columns(df: pd.DataFrame):
    return check_contract_required_columns(df, _contract(refs.CURATED_RND_STATISTICS))


def check_curated_rnd_statistics_non_empty(df: pd.DataFrame):
    return check_non_empty(df)


def check_curated_rnd_statistics_unique_grain(df: pd.DataFrame):
    return check_unique_grain(df, ["survey_year", "survey_type", "output_measure"], grain_name="curated rnd statistics grain")


def check_curated_rnd_statistics_output_measure_populated(df: pd.DataFrame):
    return check_column_non_null_non_blank(df, "output_measure", label="output_measure")


def check_curated_rnd_statistics_output_value_non_negative(df: pd.DataFrame):
    return check_numeric_column_non_negative(df, "output_value", label="output_value")


def check_curated_rnd_statistics_provenance_columns_present(df: pd.DataFrame):
    return check_required_columns_present(df, ["source_table_identifier", "source_snapshot_id", "pipeline_run_id"], label="curated provenance columns")


def check_curated_rnd_statistics_reconciles_to_site_input(curated_df: pd.DataFrame, site_df: pd.DataFrame, tolerance: float = 1e-9):
    required_ok, msg = check_required_columns_present(curated_df, ["survey_year", "survey_type", "output_measure", "output_value"], label="curated output")
    if not required_ok:
        return False, msg
    site_ok, site_msg = check_required_columns_present(site_df, ["survey_year", "survey_type", "211_apportioned"], label="site apportioned input")
    if not site_ok:
        return False, site_msg
    curated_211 = curated_df[curated_df["output_measure"] == "total_211_apportioned"].copy()
    if curated_211.empty:
        return False, "No curated rows for output_measure=total_211_apportioned."
    curated_211["output_value"] = pd.to_numeric(curated_211["output_value"], errors="coerce")
    if int(curated_211["output_value"].isna().sum()):
        return False, "Curated output_value contains non-numeric/null values for total_211_apportioned."
    site_numeric = pd.to_numeric(site_df["211_apportioned"], errors="coerce")
    if int(site_numeric.isna().sum()):
        return False, "Site input 211_apportioned contains non-numeric/null values."

    site_totals = site_df.assign(_v=site_numeric).groupby(["survey_year", "survey_type"], dropna=False)["_v"].sum().reset_index(name="expected")
    curated_totals = curated_211.groupby(["survey_year", "survey_type"], dropna=False)["output_value"].sum().reset_index(name="actual")
    merged = site_totals.merge(curated_totals, how="outer", on=["survey_year", "survey_type"])
    if int(merged["expected"].isna().sum() + merged["actual"].isna().sum()):
        return False, "Curated/site reconciliation failed due to missing groups between site input and curated output."
    bad = int(((merged["expected"] - merged["actual"]).abs() > tolerance).sum())
    if bad:
        return False, f"Curated/site reconciliation failed for {bad} group(s)."
    return True, f"Curated/site reconciliation passed for {len(merged)} group(s)."

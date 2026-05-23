"""Framework-light checks for mapping output tables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"
_MAPPED_GRAIN = ["reference", "instance", "survey_type", "survey_year"]
_REQUIRED_CELL_MAPPING_COLUMNS = ["cellno", "cellnumber", "uni_count", "uni_employment"]


def _mapped_responses_contract():
    contracts = load_table_contracts(_CONTRACTS_PATH)
    return get_table_contract(contracts, refs.INTERMEDIATE_MAPPED_RESPONSES)


def _cell_number_mapped_responses_contract():
    contracts = load_table_contracts(_CONTRACTS_PATH)
    return get_table_contract(contracts, refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES)


def check_mapped_responses_required_columns(df: pd.DataFrame) -> tuple[bool, str]:
    contract = _mapped_responses_contract()
    return check_contract_required_columns(df, contract)


def check_mapped_responses_non_empty(df: pd.DataFrame) -> tuple[bool, str]:
    return check_non_empty(df)


def check_mapped_responses_unique_grain(df: pd.DataFrame) -> tuple[bool, str]:
    missing = [column for column in _MAPPED_GRAIN if column not in df.columns]
    if missing:
        return False, f"Cannot validate mapped grain uniqueness; missing columns: {', '.join(missing)}"

    duplicate_count = int(df.duplicated(subset=_MAPPED_GRAIN, keep=False).sum())
    if duplicate_count > 0:
        return (
            False,
            "Found duplicate rows at mapped grain "
            f"({'+'.join(_MAPPED_GRAIN)}): {duplicate_count} duplicate row(s).",
        )

    return True, f"Mapped grain is unique at {'+'.join(_MAPPED_GRAIN)} ({len(df)} row(s) checked)."


def check_mapped_responses_ultfoc_present(df: pd.DataFrame) -> tuple[bool, str]:
    if "ultfoc" not in df.columns:
        return False, "Cannot validate ultfoc presence; missing column: ultfoc"

    null_count = int(df["ultfoc"].isna().sum())
    blank_count = int(df["ultfoc"].astype(str).str.strip().eq("").sum())
    if null_count > 0 or blank_count > 0:
        return False, "ultfoc contains null/blank values: " f"null={null_count}, blank={blank_count}."

    return True, f"ultfoc is populated for all {len(df)} row(s)."


def check_cell_number_mapped_responses_required_columns(df: pd.DataFrame) -> tuple[bool, str]:
    contract = _cell_number_mapped_responses_contract()
    return check_contract_required_columns(df, contract)


def check_cell_number_mapped_responses_non_empty(df: pd.DataFrame) -> tuple[bool, str]:
    return check_non_empty(df)


def check_cell_number_mapped_responses_unique_grain(df: pd.DataFrame) -> tuple[bool, str]:
    return check_mapped_responses_unique_grain(df)


def check_cell_number_mapped_responses_mapping_columns_present(df: pd.DataFrame) -> tuple[bool, str]:
    missing_cols = [col for col in _REQUIRED_CELL_MAPPING_COLUMNS if col not in df.columns]
    if missing_cols:
        return False, f"Cannot validate mapped cell-number columns; missing columns: {', '.join(missing_cols)}"

    non_null_cellno = ~df["cellno"].isna()
    scoped = df.loc[non_null_cellno, ["cellnumber", "uni_count", "uni_employment"]]
    missing_mapped = scoped.isna().any(axis=1).sum()
    if int(missing_mapped) > 0:
        return (
            False,
            "Rows with non-null cellno must include cellnumber, uni_count, and uni_employment; "
            f"found {int(missing_mapped)} violating row(s).",
        )

    return (
        True,
        "Mapped cell-number columns are present and populated where response cellno is non-null.",
    )

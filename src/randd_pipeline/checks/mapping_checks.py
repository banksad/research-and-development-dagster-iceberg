"""Framework-light checks for ``intermediate.mapped_responses``."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"
_MAPPED_GRAIN = ["reference", "instance", "survey_type", "survey_year"]


def _mapped_responses_contract():
    contracts = load_table_contracts(_CONTRACTS_PATH)
    return get_table_contract(contracts, refs.INTERMEDIATE_MAPPED_RESPONSES)


def check_mapped_responses_required_columns(df: pd.DataFrame) -> tuple[bool, str]:
    """Check required columns against the mapped-responses table contract."""

    contract = _mapped_responses_contract()
    return check_contract_required_columns(df, contract)


def check_mapped_responses_non_empty(df: pd.DataFrame) -> tuple[bool, str]:
    """Check mapped responses are non-empty."""

    return check_non_empty(df)


def check_mapped_responses_unique_grain(df: pd.DataFrame) -> tuple[bool, str]:
    """Check uniqueness at ``reference+instance+survey_type+survey_year`` grain."""

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
    """Check ``ultfoc`` exists and has no null or blank values."""

    if "ultfoc" not in df.columns:
        return False, "Cannot validate ultfoc presence; missing column: ultfoc"

    null_count = int(df["ultfoc"].isna().sum())
    blank_count = int(df["ultfoc"].astype(str).str.strip().eq("").sum())
    if null_count > 0 or blank_count > 0:
        return (
            False,
            "ultfoc contains null/blank values: "
            f"null={null_count}, blank={blank_count}.",
        )

    return True, f"ultfoc is populated for all {len(df)} row(s)."

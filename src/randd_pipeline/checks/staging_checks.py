"""Framework-light checks for ``intermediate.staged_responses``."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"
_STAGED_GRAIN = ["reference", "instance", "survey_type", "survey_year"]


def _staged_responses_contract():
    contracts = load_table_contracts(_CONTRACTS_PATH)
    return get_table_contract(contracts, refs.INTERMEDIATE_STAGED_RESPONSES)


def check_staged_responses_required_columns(df: pd.DataFrame) -> tuple[bool, str]:
    """Check required columns against the staged-responses table contract."""

    contract = _staged_responses_contract()
    return check_contract_required_columns(df, contract)


def check_staged_responses_non_empty(df: pd.DataFrame) -> tuple[bool, str]:
    """Check staged responses are non-empty."""

    return check_non_empty(df)


def check_staged_responses_unique_grain(df: pd.DataFrame) -> tuple[bool, str]:
    """Check uniqueness at ``reference+instance+survey_type+survey_year`` grain."""

    missing = [column for column in _STAGED_GRAIN if column not in df.columns]
    if missing:
        return False, f"Cannot validate staged grain uniqueness; missing columns: {', '.join(missing)}"

    duplicate_count = int(df.duplicated(subset=_STAGED_GRAIN, keep=False).sum())
    if duplicate_count > 0:
        return (
            False,
            "Found duplicate rows at staged grain "
            f"({'+'.join(_STAGED_GRAIN)}): {duplicate_count} duplicate row(s).",
        )

    return True, f"Staged grain is unique at {'+'.join(_STAGED_GRAIN)} ({len(df)} row(s) checked)."

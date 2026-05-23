"""Framework-light checks for ``ref.ultfoc_mapper``."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.randd_pipeline.checks.table_checks import check_contract_required_columns, check_non_empty
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"
_ULTFOC_MAPPER_GRAIN = ["ruref"]


def _ultfoc_mapper_contract():
    contracts = load_table_contracts(_CONTRACTS_PATH)
    return get_table_contract(contracts, refs.REF_ULTFOC_MAPPER)


def check_ultfoc_mapper_required_columns(df: pd.DataFrame) -> tuple[bool, str]:
    """Check required columns against the ultfoc-mapper table contract."""

    contract = _ultfoc_mapper_contract()
    return check_contract_required_columns(df, contract)


def check_ultfoc_mapper_non_empty(df: pd.DataFrame) -> tuple[bool, str]:
    """Check ultfoc mapper is non-empty."""

    return check_non_empty(df)


def check_ultfoc_mapper_unique_ruref(df: pd.DataFrame) -> tuple[bool, str]:
    """Check uniqueness at ``ruref`` grain."""

    missing = [column for column in _ULTFOC_MAPPER_GRAIN if column not in df.columns]
    if missing:
        return False, f"Cannot validate ultfoc mapper uniqueness; missing columns: {', '.join(missing)}"

    duplicate_count = int(df.duplicated(subset=_ULTFOC_MAPPER_GRAIN, keep=False).sum())
    if duplicate_count > 0:
        return False, f"Found duplicate rows at ultfoc mapper grain (ruref): {duplicate_count} duplicate row(s)."

    return True, f"ultfoc mapper grain is unique at ruref ({len(df)} row(s) checked)."

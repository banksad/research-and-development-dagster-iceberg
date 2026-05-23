"""Framework-light table contract checks for lean refoundation assets."""

from __future__ import annotations

import pandas as pd

from src.randd_pipeline.io.contracts import TableContract


def check_required_columns(df: pd.DataFrame, required_columns: list[str]) -> tuple[bool, str]:
    """Validate that ``df`` contains all required columns."""

    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        return (
            False,
            f"Missing required columns: {', '.join(missing)}",
        )

    return True, f"All required columns present ({len(required_columns)} columns checked)."


def check_non_empty(df: pd.DataFrame) -> tuple[bool, str]:
    """Validate that ``df`` is non-empty."""

    if df.empty:
        return False, "Table is empty (0 rows)."

    return True, f"Table has {len(df)} rows."


def check_contract_required_columns(
    df: pd.DataFrame,
    contract: TableContract,
) -> tuple[bool, str]:
    """Validate required columns using a loaded table contract."""

    passed, message = check_required_columns(df, contract.required_columns)
    if passed:
        return True, f"{contract.identifier}: {message}"

    return False, f"{contract.identifier}: {message}"

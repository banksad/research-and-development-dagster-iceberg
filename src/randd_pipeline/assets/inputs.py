"""Synthetic raw-input loading/materialisation helpers for early refoundation smoke tests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.randd_pipeline.io import refs
from src.randd_pipeline.io.table_store import TableStore

_REQUIRED_RAW_FULL_RESPONSES_COLUMNS = (
    "survey_year",
    "survey_type",
    "reference",
    "instance",
)


def load_raw_full_responses_from_csv(path: str | Path) -> pd.DataFrame:
    """Load a raw full-responses CSV fixture with minimal structural validation."""

    df = pd.read_csv(path)
    missing_columns = [col for col in _REQUIRED_RAW_FULL_RESPONSES_COLUMNS if col not in df.columns]
    if missing_columns:
        missing_display = ", ".join(missing_columns)
        raise ValueError(
            "raw_full_responses CSV is missing required columns: "
            f"{missing_display}. "
            f"Required columns are: {', '.join(_REQUIRED_RAW_FULL_RESPONSES_COLUMNS)}"
        )
    return df


def materialise_raw_full_responses(
    store: TableStore,
    df: pd.DataFrame,
    overwrite: bool = False,
) -> None:
    """Materialise synthetic raw full responses to ``raw.full_responses``."""

    store.create_table_from_dataframe(refs.RAW_FULL_RESPONSES, df, overwrite=overwrite)

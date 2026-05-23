"""Synthetic raw-input loading/materialisation helpers for early refoundation smoke tests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.randd_pipeline.io import refs
from src.randd_pipeline.io.table_store import TableStore
from src.randd_pipeline.resources import TableStoreResource

_REQUIRED_RAW_FULL_RESPONSES_COLUMNS = (
    "survey_year",
    "survey_type",
    "reference",
    "instance",
)


@dataclass(frozen=True)
class RawFullResponsesConfig:
    """Config contract for the raw full responses smoke asset."""

    csv_path: str

    @classmethod
    def from_mapping(cls, config: dict[str, Any]) -> "RawFullResponsesConfig":
        csv_path = config.get("csv_path")
        if not csv_path:
            raise ValueError("raw_full_responses asset requires config key 'csv_path'")
        return cls(csv_path=str(csv_path))


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


try:
    from dagster import asset
except ModuleNotFoundError:  # pragma: no cover - dagster optional in this package
    pass

else:
    @asset(config_schema={"csv_path": str})
    def raw_full_responses(
        context,
        table_store: TableStoreResource,
    ) -> dict[str, Any]:
        """Dagster smoke asset that writes synthetic raw responses to ``raw.full_responses``."""

        config = RawFullResponsesConfig.from_mapping(context.op_config)
        df = load_raw_full_responses_from_csv(config.csv_path)
        store = table_store.get_table_store()
        materialise_raw_full_responses(store, df, overwrite=False)

        return {
            "table": refs.RAW_FULL_RESPONSES,
            "csv_path": str(config.csv_path),
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
        }

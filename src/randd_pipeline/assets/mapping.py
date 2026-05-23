"""Synthetic mapping smoke asset and helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.randd_pipeline.domain.mapping.foreign_ownership import apply_foreign_ownership_mapping
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.table_store import TableStore
from src.randd_pipeline.resources import TableStoreResource

_REQUIRED_MAPPER_COLUMNS = ("ruref", "ultfoc")


@dataclass(frozen=True)
class MappedResponsesConfig:
    """Config contract for the mapped responses smoke asset."""

    ultfoc_mapper_csv_path: str

    @classmethod
    def from_mapping(cls, config: dict[str, Any]) -> "MappedResponsesConfig":
        ultfoc_mapper_csv_path = config.get("ultfoc_mapper_csv_path")
        if not ultfoc_mapper_csv_path:
            raise ValueError("mapped_responses asset requires config key 'ultfoc_mapper_csv_path'")
        return cls(ultfoc_mapper_csv_path=str(ultfoc_mapper_csv_path))


def load_ultfoc_mapper_from_csv(path: str | Path) -> pd.DataFrame:
    """Load foreign-ownership mapper fixture with minimal structural validation."""

    df = pd.read_csv(path)
    missing_columns = [col for col in _REQUIRED_MAPPER_COLUMNS if col not in df.columns]
    if missing_columns:
        missing_display = ", ".join(missing_columns)
        raise ValueError(
            f"ultfoc mapper CSV is missing required columns: {missing_display}. "
            f"Required columns are: {', '.join(_REQUIRED_MAPPER_COLUMNS)}"
        )
    return df


def materialise_mapped_responses(store: TableStore, df: pd.DataFrame, overwrite: bool = False) -> None:
    """Materialise mapped responses to ``intermediate.mapped_responses``."""

    store.create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, df, overwrite=overwrite)


try:
    from dagster import asset
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    from dagster import AssetKey

    @asset(
        key=AssetKey(["intermediate", "mapped_responses"]),
        config_schema={"ultfoc_mapper_csv_path": str},
    )
    def mapped_responses(context, table_store: TableStoreResource) -> dict[str, Any]:
        """Materialise mapped responses through the clean foreign-ownership seam."""

        config = MappedResponsesConfig.from_mapping(context.op_config)
        store = table_store.get_table_store()

        staged = store.read_table_as_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES)
        ultfoc_mapper = load_ultfoc_mapper_from_csv(config.ultfoc_mapper_csv_path)
        mapped = apply_foreign_ownership_mapping(staged_responses=staged, ultfoc_mapper=ultfoc_mapper)

        materialise_mapped_responses(store=store, df=mapped, overwrite=False)

        return {
            "table": refs.INTERMEDIATE_MAPPED_RESPONSES,
            "row_count": int(len(mapped)),
            "column_count": int(len(mapped.columns)),
        }

"""Synthetic mapping smoke assets and helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.randd_pipeline.domain.mapping.foreign_ownership import apply_foreign_ownership_mapping
from src.randd_pipeline.domain.mapping.cell_number import (
    apply_cell_number_mapping,
    canonicalise_cell_number_mapper,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.table_store import TableStore
from src.randd_pipeline.resources import TableStoreResource

_REQUIRED_MAPPER_COLUMNS = ("ruref", "ultfoc")


@dataclass(frozen=True)
class UltfocMapperConfig:
    """Config contract for the ultfoc mapper smoke asset."""

    ultfoc_mapper_csv_path: str

    @classmethod
    def from_mapping(cls, config: dict[str, Any]) -> "UltfocMapperConfig":
        ultfoc_mapper_csv_path = config.get("ultfoc_mapper_csv_path")
        if not ultfoc_mapper_csv_path:
            raise ValueError("ultfoc_mapper asset requires config key 'ultfoc_mapper_csv_path'")
        return cls(ultfoc_mapper_csv_path=str(ultfoc_mapper_csv_path))


@dataclass(frozen=True)
class CellNumberMapperConfig:
    cell_number_mapper_csv_path: str

    @classmethod
    def from_mapping(cls, config: dict[str, Any]) -> "CellNumberMapperConfig":
        cell_number_mapper_csv_path = config.get("cell_number_mapper_csv_path")
        if not cell_number_mapper_csv_path:
            raise ValueError(
                "cell_number_mapper asset requires config key 'cell_number_mapper_csv_path'"
            )
        return cls(cell_number_mapper_csv_path=str(cell_number_mapper_csv_path))


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


def load_cell_number_mapper_from_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path)


def materialise_ultfoc_mapper(store: TableStore, df: pd.DataFrame, overwrite: bool = False) -> None:
    """Materialise ultfoc mapper to ``ref.ultfoc_mapper``."""

    store.create_table_from_dataframe(refs.REF_ULTFOC_MAPPER, df, overwrite=overwrite)


def materialise_mapped_responses(store: TableStore, df: pd.DataFrame, overwrite: bool = False) -> None:
    """Materialise mapped responses to ``intermediate.mapped_responses``."""

    store.create_table_from_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES, df, overwrite=overwrite)


def materialise_cell_number_mapper(store: TableStore, df: pd.DataFrame, overwrite: bool = False) -> None:
    store.create_table_from_dataframe(refs.REF_CELL_NUMBER_MAPPER, df, overwrite=overwrite)


def materialise_cell_number_mapped_responses(
    store: TableStore, df: pd.DataFrame, overwrite: bool = False
) -> None:
    store.create_table_from_dataframe(
        refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES,
        df,
        overwrite=overwrite,
    )


try:
    from dagster import asset
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    from dagster import AssetKey

    @asset(
        key=AssetKey(["ref", "ultfoc_mapper"]),
        config_schema={"ultfoc_mapper_csv_path": str},
    )
    def ultfoc_mapper(context, table_store: TableStoreResource) -> dict[str, Any]:
        """Materialise foreign ownership mapper reference table."""

        config = UltfocMapperConfig.from_mapping(context.op_config)
        store = table_store.get_table_store()

        mapper = load_ultfoc_mapper_from_csv(config.ultfoc_mapper_csv_path)
        materialise_ultfoc_mapper(store=store, df=mapper, overwrite=False)

        return {
            "table": refs.REF_ULTFOC_MAPPER,
            "row_count": int(len(mapper)),
            "column_count": int(len(mapper.columns)),
        }

    @asset(key=AssetKey(["intermediate", "mapped_responses"]))
    def mapped_responses(table_store: TableStoreResource) -> dict[str, Any]:
        """Materialise canonical mapped responses for the current v1 mapping scope."""

        store = table_store.get_table_store()

        staged = store.read_table_as_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES)
        ultfoc_mapper_df = store.read_table_as_dataframe(refs.REF_ULTFOC_MAPPER)
        foreign_ownership_mapped = apply_foreign_ownership_mapping(
            staged_responses=staged, ultfoc_mapper=ultfoc_mapper_df
        )
        cell_number_mapper_df = store.read_table_as_dataframe(refs.REF_CELL_NUMBER_MAPPER)
        mapped = apply_cell_number_mapping(
            responses=foreign_ownership_mapped,
            cell_number_mapper=cell_number_mapper_df,
        )

        materialise_mapped_responses(store=store, df=mapped, overwrite=False)

        return {
            "table": refs.INTERMEDIATE_MAPPED_RESPONSES,
            "row_count": int(len(mapped)),
            "column_count": int(len(mapped.columns)),
        }

    @asset(
        key=AssetKey(["ref", "cell_number_mapper"]),
        config_schema={"cell_number_mapper_csv_path": str},
    )
    def cell_number_mapper(context, table_store: TableStoreResource) -> dict[str, Any]:
        config = CellNumberMapperConfig.from_mapping(context.op_config)
        store = table_store.get_table_store()
        raw_mapper = load_cell_number_mapper_from_csv(config.cell_number_mapper_csv_path)
        mapper = canonicalise_cell_number_mapper(raw_mapper)
        materialise_cell_number_mapper(store=store, df=mapper, overwrite=False)
        return {
            "table": refs.REF_CELL_NUMBER_MAPPER,
            "row_count": int(len(mapper)),
            "column_count": int(len(mapper.columns)),
        }

    @asset(key=AssetKey(["intermediate", "cell_number_mapped_responses"]))
    def cell_number_mapped_responses(table_store: TableStoreResource) -> dict[str, Any]:
        """Materialise temporary checkpoint/debug mapped responses for transition only."""

        store = table_store.get_table_store()
        mapped_responses_df = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
        mapper_df = store.read_table_as_dataframe(refs.REF_CELL_NUMBER_MAPPER)
        mapped = apply_cell_number_mapping(
            responses=mapped_responses_df,
            cell_number_mapper=mapper_df,
        )
        materialise_cell_number_mapped_responses(store=store, df=mapped, overwrite=False)
        return {
            "table": refs.INTERMEDIATE_CELL_NUMBER_MAPPED_RESPONSES,
            "row_count": int(len(mapped)),
            "column_count": int(len(mapped.columns)),
        }

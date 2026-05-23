"""PyIceberg-backed table store implementation for local refoundation development/testing."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog as pyiceberg_load_catalog
from pyiceberg.exceptions import NoSuchTableError

from src.randd_pipeline.io.catalog_config import CatalogConfig
from src.randd_pipeline.io.table_store import TableStore


def load_catalog(config: CatalogConfig) -> Catalog:
    """Load a PyIceberg catalog from CatalogConfig."""

    properties = dict(config.properties)

    if config.type == "local_sql":
        return pyiceberg_load_catalog(
            config.name,
            type="sql",
            uri=properties["uri"],
            warehouse=properties["warehouse"],
        )

    if config.type == "rest":
        # Configuration only in this PR; no network calls are made by tests.
        kwargs = {"type": "rest", "uri": properties["uri"]}
        if config.warehouse:
            kwargs["warehouse"] = config.warehouse
        for key, value in properties.items():
            if key not in {"uri", "warehouse"}:
                kwargs[key] = value
        return pyiceberg_load_catalog(config.name, **kwargs)

    raise ValueError(f"Unsupported catalog config type: {config.type}")


class LocalPyIcebergTableStore(TableStore):
    """Local SQL catalog-backed table store for dev/test environments only."""

    def __init__(self, config: CatalogConfig) -> None:
        if config.type != "local_sql":
            raise ValueError("LocalPyIcebergTableStore requires local_sql catalog config")

        warehouse = config.warehouse
        if warehouse is None:
            raise ValueError("local_sql catalog config must include warehouse path")

        Path(warehouse).mkdir(parents=True, exist_ok=True)
        self.catalog = load_catalog(config)

    def table_exists(self, identifier: str) -> bool:
        try:
            self.catalog.load_table(identifier)
            return True
        except NoSuchTableError:
            return False

    def ensure_namespace(self, namespace: str) -> None:
        try:
            self.catalog.create_namespace(namespace)
        except Exception as err:  # pyiceberg raises generic ValueError on duplicates in some versions
            if "already exists" not in str(err).lower():
                raise

    def create_table_from_dataframe(
        self,
        identifier: str,
        df: pd.DataFrame,
        overwrite: bool = False,
    ) -> None:
        namespace = identifier.split(".", 1)[0]
        self.ensure_namespace(namespace)

        if overwrite and self.table_exists(identifier):
            self.catalog.drop_table(identifier)

        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        if self.table_exists(identifier):
            table = self.catalog.load_table(identifier)
            table.overwrite(arrow_table)
            return

        self.catalog.create_table(identifier=identifier, schema=arrow_table.schema)
        table = self.catalog.load_table(identifier)
        table.append(arrow_table)

    def append_dataframe(self, identifier: str, df: pd.DataFrame) -> None:
        table = self.catalog.load_table(identifier)
        table.append(pa.Table.from_pandas(df, preserve_index=False))

    def read_table_as_dataframe(self, identifier: str) -> pd.DataFrame:
        table = self.catalog.load_table(identifier)
        return table.scan().to_arrow().to_pandas()

"""Dagster resource boundary for refoundation table-store persistence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from src.randd_pipeline.io.catalog_config import CatalogConfig
from src.randd_pipeline.io.iceberg import LocalPyIcebergTableStore
from src.randd_pipeline.io.table_store import TableStore


try:
    from dagster import ConfigurableResource
    from pydantic import Field
except ModuleNotFoundError:  # pragma: no cover - fallback for environments without dagster
    @dataclass
    class TableStoreResource:
        """Fallback resource type when Dagster is unavailable."""

        catalog_name: str
        catalog_type: Literal["local_sql", "rest"]
        warehouse: str | None = None
        uri: str | None = None
        properties: dict[str, str] = field(default_factory=dict)

        def get_table_store(self) -> TableStore:
            config = CatalogConfig(
                name=self.catalog_name,
                type=self.catalog_type,
                warehouse=self.warehouse,
                uri=self.uri,
                properties=dict(self.properties),
            )

            if self.catalog_type == "local_sql":
                return LocalPyIcebergTableStore(config)

            raise NotImplementedError(
                "REST/lakehouse-service TableStore is a future implementation and "
                "is intentionally not implemented in this refoundation scaffold."
            )

else:
    class TableStoreResource(ConfigurableResource):
        """Configurable resource that builds a refoundation ``TableStore`` implementation."""

        catalog_name: str
        catalog_type: Literal["local_sql", "rest"]
        warehouse: str | None = None
        uri: str | None = None
        properties: dict[str, str] = Field(default_factory=dict)

        def get_table_store(self) -> TableStore:
            """Construct a table-store instance from resource configuration."""

            config = CatalogConfig(
                name=self.catalog_name,
                type=self.catalog_type,
                warehouse=self.warehouse,
                uri=self.uri,
                properties=dict(self.properties),
            )

            if self.catalog_type == "local_sql":
                return LocalPyIcebergTableStore(config)

            raise NotImplementedError(
                "REST/lakehouse-service TableStore is a future implementation and "
                "is intentionally not implemented in this refoundation scaffold."
            )

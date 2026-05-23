"""Dagster resource boundary for refoundation table-store persistence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from src.randd_pipeline.io.catalog_config import CatalogConfig, local_sql_catalog_config
from src.randd_pipeline.io.iceberg import LocalPyIcebergTableStore
from src.randd_pipeline.io.table_store import TableStore


def _normalise_catalog_config(
    *,
    catalog_name: str,
    catalog_type: Literal["local_sql", "rest"],
    warehouse: str | None,
    uri: str | None,
    properties: dict[str, str],
) -> CatalogConfig:
    """Normalise resource fields into a catalog config for store construction."""

    if catalog_type == "local_sql":
        if warehouse is None:
            raise ValueError("local_sql catalog requires a warehouse path")

        base_config = local_sql_catalog_config(catalog_name, warehouse)
        normalised_properties = dict(base_config.properties)

        # Keep explicit URI overrides for dev/test when provided by callers.
        if uri is not None:
            normalised_properties["uri"] = uri

        normalised_properties.update(properties)

        resolved_uri = normalised_properties.get("uri", base_config.uri)

        return CatalogConfig(
            name=base_config.name,
            type=base_config.type,
            warehouse=base_config.warehouse,
            uri=resolved_uri,
            properties=normalised_properties,
        )

    return CatalogConfig(
        name=catalog_name,
        type=catalog_type,
        warehouse=warehouse,
        uri=uri,
        properties=dict(properties),
    )


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
            config = _normalise_catalog_config(
                catalog_name=self.catalog_name,
                catalog_type=self.catalog_type,
                warehouse=self.warehouse,
                uri=self.uri,
                properties=self.properties,
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

            config = _normalise_catalog_config(
                catalog_name=self.catalog_name,
                catalog_type=self.catalog_type,
                warehouse=self.warehouse,
                uri=self.uri,
                properties=self.properties,
            )

            if self.catalog_type == "local_sql":
                return LocalPyIcebergTableStore(config)

            raise NotImplementedError(
                "REST/lakehouse-service TableStore is a future implementation and "
                "is intentionally not implemented in this refoundation scaffold."
            )

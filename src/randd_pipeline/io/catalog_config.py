"""Catalog configuration models for refoundation table storage."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


CatalogType = Literal["local_sql", "rest"]


@dataclass(frozen=True)
class CatalogConfig:
    """Lightweight config for table catalog construction."""

    name: str
    type: CatalogType
    warehouse: str | None = None
    uri: str | None = None
    properties: dict[str, str] = field(default_factory=dict)


def local_sql_catalog_config(name: str, warehouse_path: str | Path) -> CatalogConfig:
    """Build a local SQLite-backed PyIceberg SQL catalog config for dev/test use only."""

    warehouse = Path(warehouse_path)
    db_path = warehouse / "catalog.sqlite"
    uri = f"sqlite:///{db_path}"

    return CatalogConfig(
        name=name,
        type="local_sql",
        warehouse=str(warehouse),
        uri=uri,
        properties={"uri": uri, "warehouse": str(warehouse)},
    )


def rest_catalog_config(
    name: str,
    uri: str,
    warehouse: str | None = None,
    properties: dict[str, str] | None = None,
) -> CatalogConfig:
    """Build a REST catalog config for future lakehouse service compatibility."""

    merged_properties = {"uri": uri}
    if warehouse is not None:
        merged_properties["warehouse"] = warehouse
    if properties:
        merged_properties.update(properties)

    return CatalogConfig(
        name=name,
        type="rest",
        warehouse=warehouse,
        uri=uri,
        properties=merged_properties,
    )

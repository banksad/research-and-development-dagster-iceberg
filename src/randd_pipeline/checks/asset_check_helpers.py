from __future__ import annotations

import pandas as pd

from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult
except ModuleNotFoundError:  # pragma: no cover
    AssetCheckResult = None  # type: ignore[assignment]


def read_table_for_check(
    table_store: TableStoreResource, table_identifier: str, *, check_label: str
) -> tuple[pd.DataFrame | None, AssetCheckResult | None]:
    if AssetCheckResult is None:  # pragma: no cover
        raise RuntimeError("read_table_for_check requires dagster to create AssetCheckResult.")

    store = table_store.get_table_store()
    if not store.table_exists(table_identifier):
        return None, AssetCheckResult(
            passed=False,
            description=f"Table {table_identifier} does not exist, cannot validate {check_label}.",
        )
    return store.read_table_as_dataframe(table_identifier), None

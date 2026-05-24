from __future__ import annotations

import pandas as pd

from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetCheckResult
except ModuleNotFoundError:  # pragma: no cover
    pass
else:

    def read_table_for_check(
        table_store: TableStoreResource, table_identifier: str, *, check_label: str
    ) -> tuple[pd.DataFrame | None, AssetCheckResult | None]:
        store = table_store.get_table_store()
        if not store.table_exists(table_identifier):
            return None, AssetCheckResult(
                passed=False,
                description=f"Table {table_identifier} does not exist, cannot validate {check_label}.",
            )
        return store.read_table_as_dataframe(table_identifier), None

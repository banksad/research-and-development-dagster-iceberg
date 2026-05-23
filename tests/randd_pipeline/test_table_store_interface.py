from typing import Any

import pandas as pd

from src.randd_pipeline.io.table_store import TableStore


class _MemoryTableStore(TableStore):
    def __init__(self) -> None:
        self._tables: dict[str, pd.DataFrame] = {}

    def table_exists(self, identifier: str) -> bool:
        return identifier in self._tables

    def ensure_namespace(self, namespace: str) -> None:
        _ = namespace

    def create_table_from_dataframe(
        self,
        identifier: str,
        df: pd.DataFrame,
        overwrite: bool = False,
    ) -> None:
        if self.table_exists(identifier) and not overwrite:
            raise ValueError("table exists")
        self._tables[identifier] = df.copy()

    def append_dataframe(self, identifier: str, df: pd.DataFrame) -> None:
        self._tables[identifier] = pd.concat([self._tables[identifier], df], ignore_index=True)

    def read_table_as_dataframe(self, identifier: str) -> pd.DataFrame:
        return self._tables[identifier].copy()


def _accept_table_store(store: TableStore) -> Any:
    return store


def test_table_store_protocol_methods_round_trip():
    store = _accept_table_store(_MemoryTableStore())
    identifier = "raw.full_responses"

    initial = pd.DataFrame([{"survey_year": 2023, "value": 12.5}])
    extra = pd.DataFrame([{"survey_year": 2023, "value": 30.0}])

    store.create_table_from_dataframe(identifier, initial)
    store.append_dataframe(identifier, extra)
    out = store.read_table_as_dataframe(identifier)

    assert store.table_exists(identifier)
    assert len(out) == 2
    assert out["value"].tolist() == [12.5, 30.0]

"""Logical table-store interface for refoundation data persistence."""

from __future__ import annotations

from typing import Protocol

import pandas as pd


class TableStore(Protocol):
    """Interface for table persistence independent of concrete catalog technology."""

    def table_exists(self, identifier: str) -> bool:
        """Return whether a table identifier is present in the backing store."""

    def ensure_namespace(self, namespace: str) -> None:
        """Ensure namespace exists in the backing store."""

    def create_table_from_dataframe(
        self,
        identifier: str,
        df: pd.DataFrame,
        overwrite: bool = False,
    ) -> None:
        """Create a new table from ``df``.

        The default behaviour is non-mutating: if ``identifier`` already exists,
        implementations should raise ``ValueError`` unless ``overwrite=True`` is
        explicitly requested.
        """

    def append_dataframe(self, identifier: str, df: pd.DataFrame) -> None:
        """Append rows to an existing table as an explicit append-only write path."""

    def read_table_as_dataframe(self, identifier: str) -> pd.DataFrame:
        """Read table contents into a pandas DataFrame."""

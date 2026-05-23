"""Table contract loading helpers for lean refoundation scaffolding."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class TableContract:
    """Minimal schema for table contract metadata."""

    identifier: str
    namespace: str
    table: str
    description: str
    layer: str
    owner: str
    expected_grain: str
    primary_key: str | None
    partition_by: list[str]
    required_columns: list[str]
    notes: list[str]


def load_table_contracts(path: str | Path) -> dict[str, TableContract]:
    """Load table contracts from a YAML contract file keyed by identifier."""

    resolved_path = Path(path)
    with resolved_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    entries = payload.get("contracts", [])
    contracts: dict[str, TableContract] = {}

    for entry in entries:
        contract = TableContract(
            identifier=entry["identifier"],
            namespace=entry["namespace"],
            table=entry["table"],
            description=entry["description"],
            layer=entry["layer"],
            owner=entry["owner"],
            expected_grain=entry["expected_grain"],
            primary_key=entry.get("primary_key"),
            partition_by=list(entry.get("partition_by", [])),
            required_columns=list(entry.get("required_columns", [])),
            notes=list(entry.get("notes", [])),
        )
        contracts[contract.identifier] = contract

    return contracts


def get_table_contract(
    contracts: dict[str, TableContract], identifier: str
) -> TableContract:
    """Return a contract by identifier with a clear KeyError if missing."""

    if identifier not in contracts:
        raise KeyError(f"Unknown table contract identifier: {identifier}")
    return contracts[identifier]

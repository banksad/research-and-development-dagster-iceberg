"""Dagster asset checks for the raw input smoke asset/table."""

from __future__ import annotations

from pathlib import Path

from src.randd_pipeline.checks.table_checks import (
    check_contract_required_columns,
    check_non_empty,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.contracts import get_table_contract, load_table_contracts
from src.randd_pipeline.resources import TableStoreResource

_CONTRACTS_PATH = Path(__file__).resolve().parents[3] / "config" / "table_contracts" / "base.yaml"


try:
    from dagster import AssetCheckResult, asset_check
except ModuleNotFoundError:  # pragma: no cover - dagster optional in this package
    pass

else:
    from dagster import AssetKey

    def _table_identifier_to_asset_key(table_identifier: str) -> AssetKey:
        """Convert ``namespace.table`` identifiers to Dagster layer/name asset keys."""

        namespace, name = table_identifier.split(".", maxsplit=1)
        return AssetKey([namespace, name])


    _RAW_FULL_RESPONSES_ASSET_KEY = _table_identifier_to_asset_key(refs.RAW_FULL_RESPONSES)

    @asset_check(asset=_RAW_FULL_RESPONSES_ASSET_KEY, name="table_exists")
    def raw_full_responses_table_exists(table_store: TableStoreResource) -> AssetCheckResult:
        """Check that ``raw.full_responses`` exists in the configured table store."""

        store = table_store.get_table_store()
        exists = store.table_exists(refs.RAW_FULL_RESPONSES)

        if exists:
            return AssetCheckResult(passed=True, description="Table raw.full_responses exists.")

        return AssetCheckResult(passed=False, description="Table raw.full_responses does not exist.")


    @asset_check(asset=_RAW_FULL_RESPONSES_ASSET_KEY, name="non_empty")
    def raw_full_responses_non_empty(table_store: TableStoreResource) -> AssetCheckResult:
        """Check that ``raw.full_responses`` contains at least one row."""

        store = table_store.get_table_store()
        if not store.table_exists(refs.RAW_FULL_RESPONSES):
            return AssetCheckResult(
                passed=False,
                description="Table raw.full_responses does not exist, cannot validate non-empty check.",
            )

        df = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES)
        passed, message = check_non_empty(df)
        return AssetCheckResult(passed=passed, description=message)


    @asset_check(asset=_RAW_FULL_RESPONSES_ASSET_KEY, name="required_columns")
    def raw_full_responses_required_columns(table_store: TableStoreResource) -> AssetCheckResult:
        """Check that ``raw.full_responses`` matches contract required-column expectations."""

        store = table_store.get_table_store()
        if not store.table_exists(refs.RAW_FULL_RESPONSES):
            return AssetCheckResult(
                passed=False,
                description="Table raw.full_responses does not exist, cannot validate required columns.",
            )

        contracts = load_table_contracts(_CONTRACTS_PATH)
        contract = get_table_contract(contracts, refs.RAW_FULL_RESPONSES)

        df = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES)
        passed, message = check_contract_required_columns(df, contract)
        return AssetCheckResult(passed=passed, description=message)

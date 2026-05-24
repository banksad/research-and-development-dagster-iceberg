"""Synthetic staging transmutation smoke asset and helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.randd_pipeline.domain.staging.transmutation import build_full_responses
from src.randd_pipeline.io import refs
from src.randd_pipeline.io.table_store import TableStore
from src.randd_pipeline.resources import TableStoreResource

_REQUIRED_CONTRIBUTOR_COLUMNS = ("reference", "instance", "survey", "period")
_REQUIRED_RESPONSES_COLUMNS = (
    "reference",
    "instance",
    "survey",
    "period",
    "questioncode",
    "response",
)


@dataclass(frozen=True)
class StagedResponsesConfig:
    """Config contract for the staged responses smoke asset."""

    contributors_csv_path: str
    responses_long_csv_path: str

    @classmethod
    def from_mapping(cls, config: dict[str, Any]) -> "StagedResponsesConfig":
        contributors_csv_path = config.get("contributors_csv_path")
        responses_long_csv_path = config.get("responses_long_csv_path")

        if not contributors_csv_path:
            raise ValueError("staged_responses asset requires config key 'contributors_csv_path'")
        if not responses_long_csv_path:
            raise ValueError("staged_responses asset requires config key 'responses_long_csv_path'")

        return cls(
            contributors_csv_path=str(contributors_csv_path),
            responses_long_csv_path=str(responses_long_csv_path),
        )


def _validate_required_columns(df: pd.DataFrame, required: tuple[str, ...], frame_name: str) -> None:
    missing_columns = [col for col in required if col not in df.columns]
    if missing_columns:
        missing_display = ", ".join(missing_columns)
        raise ValueError(
            f"{frame_name} CSV is missing required columns: {missing_display}. "
            f"Required columns are: {', '.join(required)}"
        )


def load_staging_contributors_from_csv(path: str | Path) -> pd.DataFrame:
    """Load staging contributors fixture with minimal structural validation."""

    df = pd.read_csv(path)
    _validate_required_columns(df, _REQUIRED_CONTRIBUTOR_COLUMNS, "contributors")
    return df


def load_staging_responses_long_from_csv(path: str | Path) -> pd.DataFrame:
    """Load staging long responses fixture with minimal structural validation."""

    df = pd.read_csv(path)
    _validate_required_columns(df, _REQUIRED_RESPONSES_COLUMNS, "responses")
    return df


def materialise_staged_responses(
    store: TableStore,
    df: pd.DataFrame,
    overwrite: bool = False,
) -> None:
    """Materialise staged responses to ``intermediate.staged_responses``."""

    store.create_table_from_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES, df, overwrite=overwrite)


try:
    from dagster import asset
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    from dagster import AssetKey

    @asset(
        key=AssetKey(["intermediate", "staged_responses"]),
        deps=[AssetKey(["raw", "full_responses"])],
        config_schema={
            "contributors_csv_path": str,
            "responses_long_csv_path": str,
        },
    )
    def staged_responses(context, table_store: TableStoreResource) -> dict[str, Any]:
        """Materialise staged responses via the clean transmutation seam."""

        config = StagedResponsesConfig.from_mapping(context.op_config)
        contributors = load_staging_contributors_from_csv(config.contributors_csv_path)
        responses = load_staging_responses_long_from_csv(config.responses_long_csv_path)
        full_responses = build_full_responses(contributors=contributors, responses=responses)

        store = table_store.get_table_store()
        materialise_staged_responses(store=store, df=full_responses, overwrite=False)

        return {
            "table": refs.INTERMEDIATE_STAGED_RESPONSES,
            "row_count": int(len(full_responses)),
            "column_count": int(len(full_responses.columns)),
        }

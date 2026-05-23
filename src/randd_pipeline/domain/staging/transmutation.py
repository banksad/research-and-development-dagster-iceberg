from __future__ import annotations

import pandas as pd

DEFAULT_UNIQUE_ID_COLUMNS = ["reference", "instance"]
MERGE_KEYS = ["reference", "survey", "period"]
CONTRIBUTOR_DROP_COLUMNS = ["createdby", "createddate", "lastupdatedby"]
RESPONSE_DROP_COLUMNS = [
    "createdby",
    "createddate",
    "lastupdatedby",
    "lastupdateddate",
    "adjustedresponse",
]


def _require_columns(df: pd.DataFrame, required: list[str], frame_name: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {', '.join(missing)}")


def create_response_dataframe(
    responses: pd.DataFrame,
    unique_id_columns: list[str],
) -> pd.DataFrame:
    _require_columns(responses, unique_id_columns + ["questioncode", "response"], "responses")
    response_df = responses.pivot_table(
        index=unique_id_columns,
        columns="questioncode",
        values="response",
        aggfunc="first",
    ).reset_index()
    if "instance" in response_df.columns:
        response_df = response_df.astype({"instance": "Int64"})
    return response_df


def create_contextual_dataframe(
    merged: pd.DataFrame,
    unique_id_columns: list[str],
) -> pd.DataFrame:
    _require_columns(merged, unique_id_columns + ["questioncode", "response"], "merged")
    return merged.drop(columns=["questioncode", "response"]).drop_duplicates()


def build_full_responses(
    contributors: pd.DataFrame,
    responses: pd.DataFrame,
    unique_id_columns: list[str] | None = None,
) -> pd.DataFrame:
    unique_id_columns = unique_id_columns or DEFAULT_UNIQUE_ID_COLUMNS

    _require_columns(contributors, MERGE_KEYS + unique_id_columns, "contributors")
    _require_columns(responses, MERGE_KEYS + unique_id_columns + ["questioncode", "response"], "responses")

    contributors_trimmed = contributors.drop(columns=CONTRIBUTOR_DROP_COLUMNS, errors="ignore")
    responses_trimmed = responses.drop(columns=RESPONSE_DROP_COLUMNS, errors="ignore")

    if "instance" in responses_trimmed.columns:
        responses_trimmed = responses_trimmed.astype({"instance": "Int64"})

    merged = contributors_trimmed.merge(responses_trimmed, on=MERGE_KEYS, how="outer")
    contextual = create_contextual_dataframe(merged, unique_id_columns)
    response_df = create_response_dataframe(merged, unique_id_columns)
    return response_df.merge(contextual, on=unique_id_columns, how="outer")

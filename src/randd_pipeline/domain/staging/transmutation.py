from __future__ import annotations

import pandas as pd

DEFAULT_KEY_COLUMNS = ["reference", "instance", "survey_type", "survey_year"]
LEGACY_KEY_COLUMNS = ["reference", "instance", "survey", "period"]
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
    key_columns: list[str],
) -> pd.DataFrame:
    _require_columns(responses, key_columns + ["questioncode", "response"], "responses")
    response_df = responses.pivot_table(
        index=key_columns,
        columns="questioncode",
        values="response",
        aggfunc="first",
    ).reset_index()
    if "instance" in response_df.columns:
        response_df = response_df.astype({"instance": "Int64"})
    return response_df


def create_contextual_dataframe(
    merged: pd.DataFrame,
    key_columns: list[str],
) -> pd.DataFrame:
    _require_columns(merged, key_columns + ["questioncode", "response"], "merged")
    return merged.drop(columns=["questioncode", "response"]).drop_duplicates()


def canonicalise_staging_columns(
    df: pd.DataFrame,
    *,
    survey_column: str = "survey",
    period_column: str = "period",
) -> pd.DataFrame:
    has_legacy_survey = survey_column in df.columns
    has_canonical_survey = "survey_type" in df.columns
    has_legacy_period = period_column in df.columns
    has_canonical_period = "survey_year" in df.columns

    if has_legacy_survey and has_canonical_survey:
        raise ValueError(
            f"ambiguous survey columns present: '{survey_column}' and 'survey_type'"
        )
    if has_legacy_period and has_canonical_period:
        raise ValueError(
            f"ambiguous period columns present: '{period_column}' and 'survey_year'"
        )

    rename_map: dict[str, str] = {}
    if has_legacy_survey:
        rename_map[survey_column] = "survey_type"
    if has_legacy_period:
        rename_map[period_column] = "survey_year"

    return df.rename(columns=rename_map)


def build_full_responses(
    contributors: pd.DataFrame,
    responses: pd.DataFrame,
    key_columns: list[str] | None = None,
) -> pd.DataFrame:
    key_columns = key_columns or DEFAULT_KEY_COLUMNS

    contributors_canonical = canonicalise_staging_columns(contributors)
    responses_canonical = canonicalise_staging_columns(responses)

    _require_columns(contributors_canonical, key_columns, "contributors")
    _require_columns(responses_canonical, key_columns + ["questioncode", "response"], "responses")

    contributors_trimmed = contributors_canonical.drop(columns=CONTRIBUTOR_DROP_COLUMNS, errors="ignore")
    responses_trimmed = responses_canonical.drop(columns=RESPONSE_DROP_COLUMNS, errors="ignore")

    if "instance" in responses_trimmed.columns:
        responses_trimmed = responses_trimmed.astype({"instance": "Int64"})

    merged = contributors_trimmed.merge(responses_trimmed, on=key_columns, how="outer")
    contextual = create_contextual_dataframe(merged, key_columns)
    response_df = create_response_dataframe(merged, key_columns)
    return response_df.merge(contextual, on=key_columns, how="outer")

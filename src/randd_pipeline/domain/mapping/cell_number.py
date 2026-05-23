from __future__ import annotations

import pandas as pd

_REQUIRED_MAPPER_INPUT_COLUMNS = ("cell_no", "UNI_Count", "uni_employment")
_REQUIRED_MAPPER_CANONICAL_COLUMNS = ("cellnumber", "uni_count", "uni_employment")
_REQUIRED_RESPONSES_COLUMNS = ("reference", "instance", "survey_type", "survey_year", "cellno")


def _require_columns(df: pd.DataFrame, required: tuple[str, ...], df_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {', '.join(missing)}")


def canonicalise_cell_number_mapper(mapper: pd.DataFrame) -> pd.DataFrame:
    _require_columns(mapper, _REQUIRED_MAPPER_INPUT_COLUMNS, "cell_number_mapper")
    canonical = mapper.loc[:, list(_REQUIRED_MAPPER_INPUT_COLUMNS)].rename(
        columns={"cell_no": "cellnumber", "UNI_Count": "uni_count"}
    )

    if canonical["cellnumber"].duplicated().any():
        raise ValueError("cell_number_mapper contains duplicate cellnumber values")

    bad_range = (~canonical["cellnumber"].between(1, 817, inclusive="both")) & canonical["cellnumber"].notna()
    if bad_range.any():
        raise ValueError("cell_number_mapper contains cellnumber values outside inclusive range 1..817")

    return canonical


def apply_cell_number_mapping(responses: pd.DataFrame, cell_number_mapper: pd.DataFrame) -> pd.DataFrame:
    _require_columns(responses, _REQUIRED_RESPONSES_COLUMNS, "responses")
    _require_columns(cell_number_mapper, _REQUIRED_MAPPER_CANONICAL_COLUMNS, "cell_number_mapper")

    mapped = responses.copy(deep=True).merge(
        cell_number_mapper.copy(deep=True),
        how="left",
        left_on="cellno",
        right_on="cellnumber",
    )

    unmatched = mapped["cellno"].notna() & mapped["cellnumber"].isna()
    if unmatched.any():
        values = sorted(mapped.loc[unmatched, "cellno"].dropna().unique().tolist())
        raise ValueError(f"Unmatched non-null cellno values in responses: {values}")

    if len(mapped) != len(responses):
        raise ValueError("cell-number mapping did not preserve input row count")

    return mapped

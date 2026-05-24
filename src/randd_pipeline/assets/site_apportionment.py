from __future__ import annotations

from typing import Any

import pandas as pd
from src.randd_pipeline.domain.site_apportionment import apply_explicit_site_apportionment
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource

try:
    from dagster import AssetKey, Config, Field, asset
    from pydantic import field_validator
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    class SiteApportionmentFactorsInputConfig(Config):
        site_factors_csv_path: str = Field(description="Path to a synthetic site apportionment factors CSV for local v1 testing.")

        @field_validator("site_factors_csv_path")
        @classmethod
        def _path_non_blank(cls, v: str) -> str:
            if not v.strip(): raise ValueError("site_factors_csv_path must be non-blank.")
            return v

    class SiteApportionmentConfig(Config):
        value_columns: list[str] = Field(default=["211"], description="Numeric estimated-response columns to apportion across sites.")
        output_suffix: str = Field(default="_apportioned", description="Suffix added to each apportioned output value column.")
        strict_factor_coverage: bool = Field(default=True, description="Fail the run when estimated responses do not have site factors.")
        factor_sum_tolerance: float = Field(default=1e-9, description="Tolerance for checking that site proportions sum to one per response.")

        @field_validator("value_columns")
        @classmethod
        def _value_cols_valid(cls, v: list[str]) -> list[str]:
            if not v or any(not c.strip() for c in v): raise ValueError("value_columns must be non-empty with no blank names.")
            return v

        @field_validator("output_suffix")
        @classmethod
        def _suffix_non_blank(cls, v: str) -> str:
            if not v.strip(): raise ValueError("output_suffix must be non-blank.")
            return v

        @field_validator("factor_sum_tolerance")
        @classmethod
        def _tol_pos(cls, v: float) -> float:
            if v <= 0: raise ValueError("factor_sum_tolerance must be positive.")
            return v


def materialise_site_apportionment_factors(store, df: pd.DataFrame, overwrite: bool = False) -> None:
    store.create_table_from_dataframe(refs.REF_SITE_APPORTIONMENT_FACTORS, df.copy(), overwrite=overwrite)


@asset(key=AssetKey(["ref", "site_apportionment_factors"]))
def site_apportionment_factors(table_store: TableStoreResource, config: SiteApportionmentFactorsInputConfig) -> dict[str, Any]:
    store = table_store.get_table_store()
    df = pd.read_csv(config.site_factors_csv_path)
    materialise_site_apportionment_factors(store, df, overwrite=False)
    return {"table": refs.REF_SITE_APPORTIONMENT_FACTORS, "row_count": int(len(df)), "column_count": int(len(df.columns))}


@asset(key=AssetKey(["intermediate", "site_apportioned_responses"]), deps=[AssetKey(["intermediate", "estimated_responses"]), AssetKey(["ref", "site_apportionment_factors"])])
def site_apportioned_responses(table_store: TableStoreResource, config: SiteApportionmentConfig) -> dict[str, Any]:
    store = table_store.get_table_store()
    estimated = store.read_table_as_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES)
    factors = store.read_table_as_dataframe(refs.REF_SITE_APPORTIONMENT_FACTORS)
    out = apply_explicit_site_apportionment(estimated, factors, value_columns=config.value_columns, output_suffix=config.output_suffix, strict_factor_coverage=config.strict_factor_coverage, factor_sum_tolerance=config.factor_sum_tolerance)
    store.create_table_from_dataframe(refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, out, overwrite=False)
    split = int((out.groupby(["reference", "instance", "survey_type", "survey_year"]).size() > 1).sum())
    return {"table": refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES, "row_count": int(len(out)), "column_count": int(len(out.columns)), "input_response_count": int(len(estimated)), "output_site_row_count": int(len(out)), "split_response_count": split, "value_columns_apportioned": list(config.value_columns)}

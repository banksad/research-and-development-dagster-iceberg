from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.ref_checks import (
    check_cell_number_mapper_cellnumber_range,
    check_cell_number_mapper_non_empty,
    check_cell_number_mapper_required_columns,
    check_cell_number_mapper_unique_cellnumber,
    check_ultfoc_mapper_non_empty,
    check_ultfoc_mapper_required_columns,
    check_ultfoc_mapper_unique_ruref,
)
from src.randd_pipeline.domain.mapping.cell_number import canonicalise_cell_number_mapper
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def _ultfoc_mapper_df() -> pd.DataFrame:
    return load_scenario_csv("mapping_foreign_ownership_minimal", "ultfoc_mapper.csv")


def _cell_number_mapper_df() -> pd.DataFrame:
    return canonicalise_cell_number_mapper(load_scenario_csv("mapping_cell_number_minimal", "cell_number_mapper.csv"))


def test_ultfoc_mapper_required_columns_check_passes_for_fixture() -> None:
    assert check_ultfoc_mapper_required_columns(_ultfoc_mapper_df())[0]


@pytest.mark.parametrize("column", ["ruref", "ultfoc"])
def test_ultfoc_mapper_required_columns_check_fails_when_required_column_missing(column: str) -> None:
    passed, message = check_ultfoc_mapper_required_columns(_ultfoc_mapper_df().drop(columns=[column]))
    assert not passed
    assert f"Missing required columns: {column}" in message


def test_ultfoc_mapper_non_empty_passes_and_fails() -> None:
    assert check_ultfoc_mapper_non_empty(_ultfoc_mapper_df())[0]
    assert not check_ultfoc_mapper_non_empty(_ultfoc_mapper_df().iloc[0:0].copy())[0]


def test_ultfoc_mapper_unique_ruref_passes_and_fails() -> None:
    df = _ultfoc_mapper_df()
    assert check_ultfoc_mapper_unique_ruref(df)[0]
    dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    passed, message = check_ultfoc_mapper_unique_ruref(dup)
    assert not passed
    assert "duplicate row(s)" in message


def test_cell_number_mapper_required_columns_check_passes_for_fixture() -> None:
    passed, _ = check_cell_number_mapper_required_columns(_cell_number_mapper_df())
    assert passed


@pytest.mark.parametrize("column", ["cellnumber", "uni_count", "uni_employment"])
def test_cell_number_mapper_required_columns_check_fails_when_required_column_missing(column: str) -> None:
    passed, message = check_cell_number_mapper_required_columns(_cell_number_mapper_df().drop(columns=[column]))
    assert not passed
    assert f"Missing required columns: {column}" in message


def test_cell_number_mapper_non_empty_passes_and_fails() -> None:
    assert check_cell_number_mapper_non_empty(_cell_number_mapper_df())[0]
    assert not check_cell_number_mapper_non_empty(_cell_number_mapper_df().iloc[0:0].copy())[0]


def test_cell_number_mapper_unique_cellnumber_passes_and_fails() -> None:
    df = _cell_number_mapper_df()
    assert check_cell_number_mapper_unique_cellnumber(df)[0]
    dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    passed, message = check_cell_number_mapper_unique_cellnumber(dup)
    assert not passed
    assert "duplicate row(s)" in message


def test_cell_number_mapper_range_passes() -> None:
    assert check_cell_number_mapper_cellnumber_range(_cell_number_mapper_df())[0]


@pytest.mark.parametrize("bad_value", [0, 818])
def test_cell_number_mapper_range_fails_outside_allowed_range(bad_value: int) -> None:
    df = _cell_number_mapper_df().copy()
    df.loc[0, "cellnumber"] = bad_value
    passed, message = check_cell_number_mapper_cellnumber_range(df)
    assert not passed
    assert "inclusive range 1..817" in message


@pytest.mark.parametrize("bad_value", [None, "   "])
def test_cell_number_mapper_range_fails_for_null_blank_cellnumber(bad_value: object) -> None:
    df = _cell_number_mapper_df().copy()
    df.loc[0, "cellnumber"] = bad_value
    passed, message = check_cell_number_mapper_cellnumber_range(df)
    assert not passed
    assert "null/blank" in message


def test_ref_checks_module_does_not_import_legacy_modules() -> None:
    importlib.import_module("src.randd_pipeline.checks.ref_checks")
    banned_prefixes = ["src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]
    assert not any(
        name == prefix or name.startswith(f"{prefix}.")
        for name in importlib.sys.modules
        for prefix in banned_prefixes
    )

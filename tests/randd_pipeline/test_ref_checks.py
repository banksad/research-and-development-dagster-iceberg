from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.ref_checks import (
    check_ultfoc_mapper_non_empty,
    check_ultfoc_mapper_required_columns,
    check_ultfoc_mapper_unique_ruref,
)
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

SCENARIO_ID = "mapping_foreign_ownership_minimal"


def _ultfoc_mapper_df() -> pd.DataFrame:
    return load_scenario_csv(SCENARIO_ID, "ultfoc_mapper.csv")


def test_ultfoc_mapper_required_columns_check_passes_for_fixture() -> None:
    passed, message = check_ultfoc_mapper_required_columns(_ultfoc_mapper_df())
    assert passed
    assert "ref.ultfoc_mapper" in message


def test_ultfoc_mapper_required_columns_check_fails_clearly_when_columns_missing() -> None:
    missing_ruref = _ultfoc_mapper_df().drop(columns=["ruref"])
    passed_ruref, message_ruref = check_ultfoc_mapper_required_columns(missing_ruref)
    assert not passed_ruref
    assert "Missing required columns: ruref" in message_ruref

    missing_ultfoc = _ultfoc_mapper_df().drop(columns=["ultfoc"])
    passed_ultfoc, message_ultfoc = check_ultfoc_mapper_required_columns(missing_ultfoc)
    assert not passed_ultfoc
    assert "Missing required columns: ultfoc" in message_ultfoc


def test_ultfoc_mapper_non_empty_check_passes_and_fails_clearly() -> None:
    passed, message = check_ultfoc_mapper_non_empty(_ultfoc_mapper_df())
    assert passed
    assert "Table has" in message

    failed, fail_message = check_ultfoc_mapper_non_empty(_ultfoc_mapper_df().iloc[0:0].copy())
    assert not failed
    assert "Table is empty" in fail_message


def test_ultfoc_mapper_unique_ruref_check_passes_for_fixture() -> None:
    passed, message = check_ultfoc_mapper_unique_ruref(_ultfoc_mapper_df())
    assert passed
    assert "unique at ruref" in message


def test_ultfoc_mapper_unique_ruref_check_fails_clearly_for_duplicate_rows() -> None:
    df = _ultfoc_mapper_df()
    duplicate_df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    passed, message = check_ultfoc_mapper_unique_ruref(duplicate_df)
    assert not passed
    assert "duplicate row(s)" in message

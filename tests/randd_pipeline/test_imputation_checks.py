from __future__ import annotations

import pytest

pytest.importorskip("pandas")

from src.randd_pipeline.checks.imputation_checks import (
    check_imputation_marker_populated,
    check_imputed_responses_non_empty,
    check_imputed_responses_required_columns,
    check_imputed_responses_unique_grain,
    check_no_illegal_missing_imputed_values,
)
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def test_imputation_checks_pass_on_expected_fixture() -> None:
    df = load_scenario_csv("mapping_to_imputation_minimal", "expected_imputed_responses.csv")
    for fn in [
        check_imputed_responses_required_columns,
        check_imputed_responses_non_empty,
        check_imputed_responses_unique_grain,
        check_imputation_marker_populated,
        check_no_illegal_missing_imputed_values,
    ]:
        ok, _ = fn(df)
        assert ok

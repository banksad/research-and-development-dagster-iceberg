from __future__ import annotations

import pytest

pytest.importorskip("pandas")

from src.randd_pipeline.checks.outlier_checks import check_outlier_source_populated
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def test_outlier_source_values() -> None:
    df = load_scenario_csv("imputation_to_outlier_minimal", "expected_outlier_adjusted_responses.csv")
    passed, _ = check_outlier_source_populated(df)
    assert passed
    df.loc[0, "outlier_source"] = "bad"
    passed, _ = check_outlier_source_populated(df)
    assert not passed

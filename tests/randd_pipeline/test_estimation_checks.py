from __future__ import annotations

from src.randd_pipeline.checks.estimation_checks import *
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def test_estimation_checks_pass_fixture() -> None:
    df = load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv")
    for fn in [check_estimated_responses_required_columns, check_estimated_responses_non_empty, check_estimated_responses_unique_grain, check_estimation_weights_populated, check_estimation_weights_positive]:
        assert fn(df)[0]

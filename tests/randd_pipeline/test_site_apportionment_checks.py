from __future__ import annotations
from src.randd_pipeline.checks.site_apportionment_checks import *
from tests.randd_pipeline.fixture_helpers import load_scenario_csv

def test_site_checks_pass():
    out=load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv")
    fac=load_scenario_csv("estimation_to_site_apportionment_minimal", "input_site_apportionment_factors.csv")
    assert check_site_apportioned_required_columns(out)[0]
    assert check_site_apportioned_non_empty(out)[0]
    assert check_site_apportioned_unique_site_grain(out)[0]
    assert check_site_identifier_populated(out)[0]
    assert check_site_proportion_populated(out)[0]
    assert check_apportioned_values_non_negative(out)[0]
    assert check_site_factors_required_columns(fac)[0]
    assert check_site_factors_unique_site_grain(fac)[0]
    assert check_site_factors_proportions_sum_to_one(fac)[0]

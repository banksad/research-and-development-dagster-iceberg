from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.site_apportionment_checks import *
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def test_site_apportioned_checks_pass_and_fail_cases():
    out = load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv")
    assert check_site_apportioned_required_columns(out)[0]
    assert check_site_apportioned_non_empty(out)[0]
    assert check_site_apportioned_unique_site_grain(out)[0]
    assert check_site_identifier_populated(out)[0]
    assert check_site_proportion_populated(out)[0]
    assert check_apportioned_values_non_negative(out)[0]

    assert not check_site_apportioned_required_columns(out.drop(columns=["site_id"]))[0]
    assert not check_site_apportioned_non_empty(out.iloc[0:0])[0]
    assert not check_site_apportioned_unique_site_grain(pd.concat([out, out.iloc[[0]]], ignore_index=True))[0]
    bad_id = out.copy(); bad_id.loc[0, "site_id"] = " "
    assert not check_site_identifier_populated(bad_id)[0]
    bad_prop = out.copy(); bad_prop.loc[0, "site_proportion"] = -0.1
    assert not check_site_proportion_populated(bad_prop)[0]
    bad_val = out.copy(); bad_val.loc[0, "211_apportioned"] = -1
    assert not check_apportioned_values_non_negative(bad_val)[0]


def test_factor_checks_pass_and_fail_cases():
    fac = load_scenario_csv("estimation_to_site_apportionment_minimal", "input_site_apportionment_factors.csv")
    assert check_site_factors_required_columns(fac)[0]
    assert check_site_factors_unique_site_grain(fac)[0]
    assert check_site_identifier_populated(fac)[0]
    assert check_site_factors_proportions_sum_to_one(fac)[0]

    assert not check_site_factors_required_columns(fac.drop(columns=["site_proportion"]))[0]
    assert not check_site_factors_unique_site_grain(pd.concat([fac, fac.iloc[[0]]], ignore_index=True))[0]
    bad_id = fac.copy(); bad_id.loc[0, "site_id"] = ""
    assert not check_site_identifier_populated(bad_id)[0]
    bad_sum = fac.copy(); bad_sum.loc[0, "site_proportion"] = 0.2
    assert not check_site_factors_proportions_sum_to_one(bad_sum)[0]
    ok, msg = check_site_factors_proportions_sum_to_one(fac.drop(columns=["survey_year"]))
    assert not ok and "missing columns" in msg

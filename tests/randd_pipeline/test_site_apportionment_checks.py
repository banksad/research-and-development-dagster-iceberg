from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.site_apportionment_checks import *
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def _out():
    return load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv")


def _fac():
    return load_scenario_csv("estimation_to_site_apportionment_minimal", "input_site_apportionment_factors.csv")


def test_site_apportioned_required_columns_positive_and_negative():
    ok, _ = check_site_apportioned_required_columns(_out()); assert ok
    assert not check_site_apportioned_required_columns(_out().drop(columns=["site_id"]))[0]
    assert not check_site_apportioned_required_columns(_out().drop(columns=["211_apportioned"]))[0]
    assert not check_site_apportioned_required_columns(_out().drop(columns=["instance"]))[0]


def test_site_apportioned_non_empty_positive_and_negative():
    assert check_site_apportioned_non_empty(_out())[0]
    assert not check_site_apportioned_non_empty(_out().iloc[0:0])[0]


def test_site_apportioned_unique_site_grain_positive_and_negative():
    assert check_site_apportioned_unique_site_grain(_out())[0]
    assert not check_site_apportioned_unique_site_grain(pd.concat([_out(), _out().iloc[[0]]], ignore_index=True))[0]


def test_site_identifier_populated_positive_and_negative():
    assert check_site_identifier_populated(_out())[0]
    bad = _out(); bad.loc[0, "site_id"] = None; assert not check_site_identifier_populated(bad)[0]
    bad = _out(); bad.loc[0, "site_id"] = "   "; assert not check_site_identifier_populated(bad)[0]


def test_site_proportion_populated_positive_and_negative():
    assert check_site_proportion_populated(_out())[0]
    bad = _out(); bad.loc[0, "site_proportion"] = None; assert not check_site_proportion_populated(bad)[0]
    bad = _out(); bad.loc[0, "site_proportion"] = "x"; assert not check_site_proportion_populated(bad)[0]
    bad = _out(); bad.loc[0, "site_proportion"] = -0.2; assert not check_site_proportion_populated(bad)[0]
    ok, msg = check_site_proportion_populated(_out().drop(columns=["site_proportion"])); assert not ok and "Missing" in msg


def test_apportioned_values_non_negative_positive_and_negative():
    assert check_apportioned_values_non_negative(_out())[0]
    bad = _out(); bad.loc[0, "211_apportioned"] = None; assert not check_apportioned_values_non_negative(bad)[0]
    bad = _out(); bad.loc[0, "211_apportioned"] = "x"; assert not check_apportioned_values_non_negative(bad)[0]
    bad = _out(); bad.loc[0, "211_apportioned"] = -1; assert not check_apportioned_values_non_negative(bad)[0]
    ok, msg = check_apportioned_values_non_negative(_out().drop(columns=["211_apportioned"])); assert not ok and "Missing apportioned column" in msg


def test_site_factors_required_columns_positive_and_negative():
    assert check_site_factors_required_columns(_fac())[0]
    assert not check_site_factors_required_columns(_fac().drop(columns=["site_id"]))[0]
    assert not check_site_factors_required_columns(_fac().drop(columns=["site_proportion"]))[0]
    assert not check_site_factors_required_columns(_fac().drop(columns=["instance"]))[0]


def test_site_factors_unique_site_grain_positive_and_negative():
    assert check_site_factors_unique_site_grain(_fac())[0]
    assert not check_site_factors_unique_site_grain(pd.concat([_fac(), _fac().iloc[[0]]], ignore_index=True))[0]


def test_site_identifier_populated_for_factors_positive_and_negative():
    assert check_site_identifier_populated(_fac())[0]
    bad = _fac(); bad.loc[0, "site_id"] = None; assert not check_site_identifier_populated(bad)[0]
    bad = _fac(); bad.loc[0, "site_id"] = " "; assert not check_site_identifier_populated(bad)[0]


def test_site_factors_proportions_sum_to_one_positive_and_negative():
    assert check_site_factors_proportions_sum_to_one(_fac())[0]
    low = _fac(); low.loc[low["reference"] == "SYN001", "site_proportion"] = [0.2, 0.2]
    assert not check_site_factors_proportions_sum_to_one(low)[0]
    high = _fac(); high.loc[high["reference"] == "SYN001", "site_proportion"] = [0.8, 0.8]
    assert not check_site_factors_proportions_sum_to_one(high)[0]
    ok, msg = check_site_factors_proportions_sum_to_one(_fac().drop(columns=["survey_year"])); assert not ok and "missing columns" in msg
    bad = _fac(); bad.loc[0, "site_proportion"] = "x"
    ok, msg = check_site_factors_proportions_sum_to_one(bad); assert not ok and "non-numeric" in msg

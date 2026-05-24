from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.estimation_checks import (
    check_estimated_responses_non_empty,
    check_estimated_responses_required_columns,
    check_estimated_responses_unique_grain,
    check_estimation_weights_populated,
    check_estimation_weights_positive,
)
from tests.randd_pipeline.fixture_helpers import load_scenario_csv


def _expected_df():
    return load_scenario_csv("outlier_to_estimation_minimal", "expected_estimated_responses.csv")


def test_required_columns_pass_fixture() -> None:
    assert check_estimated_responses_required_columns(_expected_df())[0]


def test_required_columns_fail_for_missing_weight_or_grain() -> None:
    df = _expected_df()
    assert not check_estimated_responses_required_columns(df.drop(columns=["a_weight"]))[0]
    assert not check_estimated_responses_required_columns(df.drop(columns=["g_weight"]))[0]
    assert not check_estimated_responses_required_columns(df.drop(columns=["reference"]))[0]


def test_non_empty_check() -> None:
    df = _expected_df()
    assert check_estimated_responses_non_empty(df)[0]
    assert not check_estimated_responses_non_empty(df.iloc[0:0])[0]


def test_unique_grain_check() -> None:
    df = _expected_df()
    assert check_estimated_responses_unique_grain(df)[0]
    with_dupe = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    assert not check_estimated_responses_unique_grain(with_dupe)[0]


def test_weights_populated_check() -> None:
    df = _expected_df()
    assert check_estimation_weights_populated(df)[0]

    a_null = df.copy()
    a_null.loc[a_null.index[0], "a_weight"] = None
    assert not check_estimation_weights_populated(a_null)[0]

    g_null = df.copy()
    g_null.loc[g_null.index[0], "g_weight"] = None
    assert not check_estimation_weights_populated(g_null)[0]

    assert not check_estimation_weights_populated(df.drop(columns=["a_weight"]))[0]
    assert not check_estimation_weights_populated(df.drop(columns=["g_weight"]))[0]


def test_weights_positive_check() -> None:
    df = _expected_df()
    assert check_estimation_weights_positive(df)[0]

    zero_a = df.copy()
    zero_a.loc[zero_a.index[0], "a_weight"] = 0
    assert not check_estimation_weights_positive(zero_a)[0]

    neg_g = df.copy()
    neg_g.loc[neg_g.index[0], "g_weight"] = -1
    assert not check_estimation_weights_positive(neg_g)[0]

    non_numeric = df.copy()
    non_numeric.loc[non_numeric.index[0], "a_weight"] = "bad"
    assert not check_estimation_weights_positive(non_numeric)[0]

    inf_weight = df.copy()
    inf_weight.loc[inf_weight.index[0], "g_weight"] = float("inf")
    assert not check_estimation_weights_positive(inf_weight)[0]

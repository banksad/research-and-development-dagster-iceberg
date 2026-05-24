from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.common import (
    check_allowed_values,
    check_column_non_null_non_blank,
    check_group_sum_close_to,
    check_numeric_column_non_negative,
    check_numeric_column_populated,
    check_numeric_column_positive,
    check_required_columns_present,
    check_unique_grain,
)


def test_required_columns_present_pass_fail():
    df = pd.DataFrame({"a": [1], "b": [2]})
    assert check_required_columns_present(df, ["a", "b"])[0]
    ok, msg = check_required_columns_present(df, ["a", "c"], label="test frame")
    assert not ok and "missing columns" in msg


def test_unique_grain_pass_fail():
    df = pd.DataFrame({"k": [1, 2], "v": [1, 1]})
    assert check_unique_grain(df, ["k"])[0]
    assert not check_unique_grain(pd.DataFrame({"k": [1, 1]}), ["k"])[0]


def test_column_non_null_non_blank_pass_fail():
    df = pd.DataFrame({"site_id": ["A", "B"]})
    assert check_column_non_null_non_blank(df, "site_id")[0]
    assert not check_column_non_null_non_blank(pd.DataFrame({"site_id": [None]}), "site_id")[0]
    assert not check_column_non_null_non_blank(pd.DataFrame({"site_id": [" "]}), "site_id")[0]


def test_numeric_populated_null_and_non_numeric_failures():
    assert check_numeric_column_populated(pd.DataFrame({"x": [1, 2.0]}), "x")[0]
    assert not check_numeric_column_populated(pd.DataFrame({"x": [1, None]}), "x")[0]
    assert not check_numeric_column_populated(pd.DataFrame({"x": [1, "bad"]}), "x")[0]


def test_numeric_non_negative_negative_failure():
    assert check_numeric_column_non_negative(pd.DataFrame({"x": [0, 1]}), "x")[0]
    assert not check_numeric_column_non_negative(pd.DataFrame({"x": [0, -1]}), "x")[0]


def test_numeric_positive_zero_and_negative_failures():
    assert check_numeric_column_positive(pd.DataFrame({"x": [1, 2]}), "x")[0]
    assert not check_numeric_column_positive(pd.DataFrame({"x": [0, 1]}), "x")[0]
    assert not check_numeric_column_positive(pd.DataFrame({"x": [-1, 1]}), "x")[0]


def test_allowed_values_pass_fail():
    assert check_allowed_values(pd.DataFrame({"s": ["a", "b"]}), "s", {"a", "b"})[0]
    assert not check_allowed_values(pd.DataFrame({"s": ["a", "c"]}), "s", {"a", "b"})[0]


def test_group_sum_close_to_expected_pass_fail():
    df = pd.DataFrame({"g": ["A", "A", "B", "B"], "v": [0.5, 0.5, 0.3, 0.7]})
    assert check_group_sum_close_to(df, ["g"], "v", expected=1.0)[0]
    bad = pd.DataFrame({"g": ["A", "A"], "v": [0.3, 0.3]})
    assert not check_group_sum_close_to(bad, ["g"], "v", expected=1.0)[0]


def test_group_sum_close_to_non_numeric_and_missing_columns_fail_clearly():
    non_numeric = pd.DataFrame({"g": ["A", "A"], "v": [0.4, "x"]})
    ok, msg = check_group_sum_close_to(non_numeric, ["g"], "v")
    assert not ok and "non-numeric" in msg

    missing = pd.DataFrame({"g": ["A"], "x": [1]})
    ok, msg = check_group_sum_close_to(missing, ["g"], "v")
    assert not ok and "missing columns" in msg


def test_group_sum_close_to_accepts_group_columns_generator():
    df = pd.DataFrame({"g": ["A", "A", "B", "B"], "v": [0.2, 0.8, 0.4, 0.6]})
    group_cols = (c for c in ["g"])

    ok, _ = check_group_sum_close_to(df, group_cols, "v", expected=1.0)

    assert ok

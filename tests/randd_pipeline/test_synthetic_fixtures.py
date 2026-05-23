import pandas as pd
import pytest

from tests.randd_pipeline.fixture_helpers import (
    assert_frame_equal_sorted,
    load_scenario_csv,
    load_scenario_metadata,
)


SCENARIO_ID = "basic_responses"
FORBIDDEN_STRINGS = (
    "gs://",
    "gcs",
    "s3://",
    "hdfs://",
    "\\\\",
    "password",
    "secret",
    "token",
    "key=",
)
MIN_CONTRACT_COLUMNS = {"survey_year", "survey_type", "reference", "instance"}


def test_basic_responses_metadata_loads() -> None:
    metadata = load_scenario_metadata(SCENARIO_ID)
    assert metadata["scenario_id"] == SCENARIO_ID
    assert "description" in metadata
    assert "purpose" in metadata
    assert "tables_extracts_included" in metadata
    assert "expected_grain" in metadata
    assert "business_rules_exercised" in metadata


def test_basic_responses_raw_csv_loads() -> None:
    actual = load_scenario_csv(SCENARIO_ID, "raw_full_responses.csv")
    assert not actual.empty


def test_basic_responses_expected_csv_loads() -> None:
    expected = load_scenario_csv(SCENARIO_ID, "expected_raw_full_responses.csv")
    assert not expected.empty


def test_assert_frame_equal_sorted_passes_for_reordered_rows() -> None:
    left = load_scenario_csv(SCENARIO_ID, "raw_full_responses.csv")
    right = left.sample(frac=1, random_state=7).reset_index(drop=True)
    assert_frame_equal_sorted(left, right, sort_by=["survey_year", "survey_type", "reference", "instance"])


def test_assert_frame_equal_sorted_fails_for_value_difference() -> None:
    left = load_scenario_csv(SCENARIO_ID, "raw_full_responses.csv")
    right = left.copy()
    right.loc[0, "value"] = int(right.loc[0, "value"]) + 1

    with pytest.raises(AssertionError, match="DataFrame.iloc"):
        assert_frame_equal_sorted(left, right, sort_by=["survey_year", "survey_type", "reference", "instance"])


def test_fixture_content_has_no_obvious_sensitive_or_production_strings() -> None:
    raw = load_scenario_csv(SCENARIO_ID, "raw_full_responses.csv")
    expected = load_scenario_csv(SCENARIO_ID, "expected_raw_full_responses.csv")
    combined = pd.concat([raw, expected], ignore_index=True).astype(str)
    blob = "\n".join(combined.apply(lambda row: " ".join(row), axis=1)).lower()

    for marker in FORBIDDEN_STRINGS:
        assert marker not in blob


def test_raw_full_responses_has_minimum_contract_columns() -> None:
    raw = load_scenario_csv(SCENARIO_ID, "raw_full_responses.csv")
    assert MIN_CONTRACT_COLUMNS.issubset(set(raw.columns))

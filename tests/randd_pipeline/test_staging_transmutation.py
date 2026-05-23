from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.randd_pipeline.domain.staging.transmutation import (
    DEFAULT_KEY_COLUMNS,
    build_full_responses,
    create_contextual_dataframe,
    create_response_dataframe,
)
from tests.randd_pipeline.fixture_helpers import (
    assert_frame_equal_sorted,
    load_scenario_csv,
)

SCENARIO_ID = "staging_minimal_valid_responses"


def test_build_full_responses_matches_expected_fixture() -> None:
    contributors = load_scenario_csv(SCENARIO_ID, "contributors.csv")
    responses = load_scenario_csv(SCENARIO_ID, "responses_long.csv")
    expected = load_scenario_csv(SCENARIO_ID, "expected_full_responses.csv")

    actual = build_full_responses(contributors=contributors, responses=responses)

    assert_frame_equal_sorted(
        actual,
        expected,
        sort_by=["reference", "instance", "survey", "period"],
    )


def test_build_full_responses_drops_metadata_columns() -> None:
    contributors = load_scenario_csv(SCENARIO_ID, "contributors.csv")
    responses = load_scenario_csv(SCENARIO_ID, "responses_long.csv")

    actual = build_full_responses(contributors=contributors, responses=responses)

    for col in ["createdby", "createddate", "lastupdatedby", "lastupdateddate", "adjustedresponse"]:
        assert col not in actual.columns


def test_build_full_responses_has_clear_target_grain_and_no_instance_suffix_columns() -> None:
    contributors = load_scenario_csv(SCENARIO_ID, "contributors.csv")
    responses = load_scenario_csv(SCENARIO_ID, "responses_long.csv")

    actual = build_full_responses(contributors=contributors, responses=responses)

    assert "instance_x" not in actual.columns
    assert "instance_y" not in actual.columns
    assert not actual.duplicated(subset=DEFAULT_KEY_COLUMNS).any()


def test_build_full_responses_does_not_mutate_inputs() -> None:
    contributors = load_scenario_csv(SCENARIO_ID, "contributors.csv")
    responses = load_scenario_csv(SCENARIO_ID, "responses_long.csv")
    contributors_before = contributors.copy(deep=True)
    responses_before = responses.copy(deep=True)

    _ = build_full_responses(contributors=contributors, responses=responses)

    assert_frame_equal(contributors, contributors_before)
    assert_frame_equal(responses, responses_before)


def test_build_full_responses_raises_clear_error_for_missing_keys() -> None:
    contributors = load_scenario_csv(SCENARIO_ID, "contributors.csv").drop(columns=["survey"])
    responses = load_scenario_csv(SCENARIO_ID, "responses_long.csv")

    with pytest.raises(ValueError, match="contributors is missing required columns: survey"):
        build_full_responses(contributors=contributors, responses=responses)


def test_domain_module_does_not_import_legacy_staging_or_pipeline_modules() -> None:
    module_path = Path("src/randd_pipeline/domain/staging/transmutation.py")
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    banned_prefixes = ["src.staging", "src.pipeline", "freezing", "construction"]

    for banned in banned_prefixes:
        assert not any(mod == banned or mod.startswith(f"{banned}.") for mod in imported_modules)


def test_create_functions_operate_on_merged_fixture() -> None:
    contributors = load_scenario_csv(SCENARIO_ID, "contributors.csv")
    responses = load_scenario_csv(SCENARIO_ID, "responses_long.csv")

    merged = contributors.drop(columns=["createdby", "createddate", "lastupdatedby"]).merge(
        responses.drop(columns=["createdby", "createddate", "lastupdatedby", "lastupdateddate", "adjustedresponse"]),
        on=DEFAULT_KEY_COLUMNS,
        how="outer",
    )

    response_df = create_response_dataframe(merged, key_columns=DEFAULT_KEY_COLUMNS)
    contextual_df = create_contextual_dataframe(merged, key_columns=DEFAULT_KEY_COLUMNS)

    assert {"reference", "instance", "survey", "period", 601, 701}.issubset(set(response_df.columns))
    assert "questioncode" not in contextual_df.columns
    assert "response" not in contextual_df.columns


def test_duplicate_responses_use_first_value_for_same_key_and_questioncode() -> None:
    contributors = load_scenario_csv(SCENARIO_ID, "contributors.csv")
    responses = load_scenario_csv(SCENARIO_ID, "responses_long.csv")

    duplicated = pd.concat(
        [
            responses,
            pd.DataFrame(
                [
                    {
                        "reference": "SYN001",
                        "instance": 1,
                        "survey": "BERD",
                        "period": 2025,
                        "questioncode": 601,
                        "response": 999,
                        "createdby": "resp",
                        "createddate": "2026-01-05",
                        "lastupdatedby": "resp",
                        "lastupdateddate": "2026-01-06",
                        "adjustedresponse": 0,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    actual = build_full_responses(contributors=contributors, responses=duplicated)
    syn001_row = actual.loc[
        (actual["reference"] == "SYN001")
        & (actual["instance"] == 1)
        & (actual["survey"] == "BERD")
        & (actual["period"] == 2025)
    ].iloc[0]

    assert syn001_row[601] == 100

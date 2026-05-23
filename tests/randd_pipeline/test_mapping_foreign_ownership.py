from __future__ import annotations

import ast
from pathlib import Path

import pytest
from pandas.testing import assert_frame_equal

from src.randd_pipeline.domain.mapping.foreign_ownership import apply_foreign_ownership_mapping
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv

SCENARIO_ID = "mapping_foreign_ownership_minimal"
GRAIN = ["reference", "instance", "survey_type", "survey_year"]


def test_apply_foreign_ownership_mapping_matches_expected_fixture() -> None:
    staged_responses = load_scenario_csv(SCENARIO_ID, "staged_responses.csv")
    ultfoc_mapper = load_scenario_csv(SCENARIO_ID, "ultfoc_mapper.csv")
    expected = load_scenario_csv(SCENARIO_ID, "expected_mapped_responses.csv")

    actual = apply_foreign_ownership_mapping(staged_responses, ultfoc_mapper)

    assert_frame_equal_sorted(actual, expected, sort_by=GRAIN)


def test_apply_foreign_ownership_mapping_preserves_row_count_and_grain_uniqueness() -> None:
    staged_responses = load_scenario_csv(SCENARIO_ID, "staged_responses.csv")
    ultfoc_mapper = load_scenario_csv(SCENARIO_ID, "ultfoc_mapper.csv")

    actual = apply_foreign_ownership_mapping(staged_responses, ultfoc_mapper)

    assert len(actual) == len(staged_responses)
    assert not actual.duplicated(subset=GRAIN).any()


def test_apply_foreign_ownership_mapping_no_mapper_key_leakage() -> None:
    staged_responses = load_scenario_csv(SCENARIO_ID, "staged_responses.csv")
    ultfoc_mapper = load_scenario_csv(SCENARIO_ID, "ultfoc_mapper.csv")

    actual = apply_foreign_ownership_mapping(staged_responses, ultfoc_mapper)

    assert "ruref" not in actual.columns


def test_apply_foreign_ownership_mapping_defaults_missing_and_blank_mapper_values_to_gb() -> None:
    staged_responses = load_scenario_csv(SCENARIO_ID, "staged_responses.csv")
    ultfoc_mapper = load_scenario_csv(SCENARIO_ID, "ultfoc_mapper.csv")

    actual = apply_foreign_ownership_mapping(staged_responses, ultfoc_mapper)

    syn1002 = actual.loc[actual["reference"] == "SYN1002", "ultfoc"].iloc[0]
    syn1003 = actual.loc[actual["reference"] == "SYN1003", "ultfoc"].iloc[0]
    assert syn1002 == "GB"
    assert syn1003 == "GB"


def test_apply_foreign_ownership_mapping_does_not_mutate_inputs() -> None:
    staged_responses = load_scenario_csv(SCENARIO_ID, "staged_responses.csv")
    ultfoc_mapper = load_scenario_csv(SCENARIO_ID, "ultfoc_mapper.csv")
    staged_before = staged_responses.copy(deep=True)
    mapper_before = ultfoc_mapper.copy(deep=True)

    _ = apply_foreign_ownership_mapping(staged_responses, ultfoc_mapper)

    assert_frame_equal(staged_responses, staged_before)
    assert_frame_equal(ultfoc_mapper, mapper_before)


@pytest.mark.parametrize(
    ("df_name", "drop_column", "error_message"),
    [
        ("staged", "reference", "staged_responses is missing required columns: reference"),
        ("staged", "survey_year", "staged_responses is missing required columns: survey_year"),
        ("mapper", "ruref", "ultfoc_mapper is missing required columns: ruref"),
        ("mapper", "ultfoc", "ultfoc_mapper is missing required columns: ultfoc"),
    ],
)
def test_apply_foreign_ownership_mapping_raises_clear_error_for_missing_columns(
    df_name: str, drop_column: str, error_message: str
) -> None:
    staged_responses = load_scenario_csv(SCENARIO_ID, "staged_responses.csv")
    ultfoc_mapper = load_scenario_csv(SCENARIO_ID, "ultfoc_mapper.csv")

    if df_name == "staged":
        staged_responses = staged_responses.drop(columns=[drop_column])
    else:
        ultfoc_mapper = ultfoc_mapper.drop(columns=[drop_column])

    with pytest.raises(ValueError, match=error_message):
        apply_foreign_ownership_mapping(staged_responses, ultfoc_mapper)


def test_mapping_domain_module_does_not_import_legacy_modules() -> None:
    module_path = Path("src/randd_pipeline/domain/mapping/foreign_ownership.py")
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    banned_prefixes = ["src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]

    for banned in banned_prefixes:
        assert not any(mod == banned or mod.startswith(f"{banned}.") for mod in imported_modules)

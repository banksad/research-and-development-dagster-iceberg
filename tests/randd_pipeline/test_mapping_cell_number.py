from __future__ import annotations

import ast
from pathlib import Path

import pytest
from pandas.testing import assert_frame_equal

from src.randd_pipeline.domain.mapping.cell_number import (
    apply_cell_number_mapping,
    canonicalise_cell_number_mapper,
)
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv

SCENARIO_ID = "mapping_cell_number_minimal"
GRAIN = ["reference", "instance", "survey_type", "survey_year"]


def test_canonicalise_cell_number_mapper_matches_expected_canonical_columns() -> None:
    mapper = load_scenario_csv(SCENARIO_ID, "cell_number_mapper.csv")
    actual = canonicalise_cell_number_mapper(mapper)
    assert list(actual.columns) == ["cellnumber", "uni_count", "uni_employment"]


def test_canonicalise_cell_number_mapper_duplicate_cell_no_raises() -> None:
    mapper = load_scenario_csv(SCENARIO_ID, "cell_number_mapper.csv")
    mapper.loc[len(mapper)] = mapper.iloc[0]
    with pytest.raises(ValueError, match="duplicate"):
        canonicalise_cell_number_mapper(mapper)


def test_canonicalise_cell_number_mapper_out_of_range_cell_no_raises() -> None:
    mapper = load_scenario_csv(SCENARIO_ID, "cell_number_mapper.csv")
    mapper.loc[0, "cell_no"] = 0
    with pytest.raises(ValueError, match="1..817"):
        canonicalise_cell_number_mapper(mapper)


def test_apply_cell_number_mapping_matches_expected_fixture() -> None:
    responses = load_scenario_csv(SCENARIO_ID, "mapped_responses.csv")
    mapper = canonicalise_cell_number_mapper(load_scenario_csv(SCENARIO_ID, "cell_number_mapper.csv"))
    expected = load_scenario_csv(SCENARIO_ID, "expected_cell_number_mapped_responses.csv")
    actual = apply_cell_number_mapping(responses, mapper)
    assert_frame_equal_sorted(actual, expected, sort_by=GRAIN)


def test_apply_cell_number_mapping_unmatched_non_null_cellno_raises() -> None:
    responses = load_scenario_csv(SCENARIO_ID, "mapped_responses.csv")
    responses.loc[0, "cellno"] = 777
    mapper = canonicalise_cell_number_mapper(load_scenario_csv(SCENARIO_ID, "cell_number_mapper.csv"))
    with pytest.raises(ValueError, match="Unmatched non-null cellno"):
        apply_cell_number_mapping(responses, mapper)


def test_apply_cell_number_mapping_null_cellno_does_not_raise() -> None:
    responses = load_scenario_csv(SCENARIO_ID, "mapped_responses.csv")
    mapper = canonicalise_cell_number_mapper(load_scenario_csv(SCENARIO_ID, "cell_number_mapper.csv"))
    actual = apply_cell_number_mapping(responses, mapper)
    assert actual["cellno"].isna().sum() == 1


def test_apply_cell_number_mapping_does_not_mutate_inputs() -> None:
    responses = load_scenario_csv(SCENARIO_ID, "mapped_responses.csv")
    mapper_raw = load_scenario_csv(SCENARIO_ID, "cell_number_mapper.csv")
    mapper = canonicalise_cell_number_mapper(mapper_raw)
    responses_before = responses.copy(deep=True)
    mapper_before = mapper.copy(deep=True)
    _ = apply_cell_number_mapping(responses, mapper)
    assert_frame_equal(responses, responses_before)
    assert_frame_equal(mapper, mapper_before)


def test_cell_number_mapping_domain_module_does_not_import_legacy_modules() -> None:
    module_path = Path("src/randd_pipeline/domain/mapping/cell_number.py")
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

from __future__ import annotations

import pandas as pd
import pytest

from src.randd_pipeline.assets.inputs import (
    load_raw_full_responses_from_csv,
    materialise_raw_full_responses,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import (
    assert_frame_equal_sorted,
    scenario_path,
)

SCENARIO_ID = "basic_responses"


def _build_local_store(tmp_path):
    resource = TableStoreResource(
        catalog_name="raw-input-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
    )
    return resource.get_table_store()


def test_materialisation_smoke_path_from_fixture_to_table_store(tmp_path) -> None:
    raw_path = scenario_path(SCENARIO_ID) / "raw_full_responses.csv"
    expected_path = scenario_path(SCENARIO_ID) / "expected_raw_full_responses.csv"

    loaded = load_raw_full_responses_from_csv(raw_path)

    for required_column in ("survey_year", "survey_type", "reference", "instance"):
        assert required_column in loaded.columns

    store = _build_local_store(tmp_path)
    materialise_raw_full_responses(store, loaded)

    actual = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES)
    expected = pd.read_csv(expected_path)

    assert_frame_equal_sorted(actual, expected, sort_by=["reference", "instance"])


def test_load_raw_full_responses_from_csv_requires_contract_columns(tmp_path) -> None:
    broken_fixture = tmp_path / "missing_columns.csv"
    pd.DataFrame(
        {
            "survey_year": [2099],
            "survey_type": ["SYNTH"],
            "reference": ["REF001"],
        }
    ).to_csv(broken_fixture, index=False)

    with pytest.raises(ValueError, match="missing required columns: instance"):
        load_raw_full_responses_from_csv(broken_fixture)


def test_duplicate_materialisation_requires_explicit_overwrite(tmp_path) -> None:
    raw_path = scenario_path(SCENARIO_ID) / "raw_full_responses.csv"
    loaded = load_raw_full_responses_from_csv(raw_path)
    store = _build_local_store(tmp_path)

    materialise_raw_full_responses(store, loaded)

    with pytest.raises(ValueError, match="already exists"):
        materialise_raw_full_responses(store, loaded, overwrite=False)


def test_overwrite_true_replaces_existing_table_contents(tmp_path) -> None:
    raw_path = scenario_path(SCENARIO_ID) / "raw_full_responses.csv"
    loaded = load_raw_full_responses_from_csv(raw_path)
    store = _build_local_store(tmp_path)

    materialise_raw_full_responses(store, loaded)

    replacement = loaded.head(1).copy()
    replacement["reference"] = "REPLACED"

    materialise_raw_full_responses(store, replacement, overwrite=True)

    actual = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES)

    assert len(actual) == 1
    assert actual["reference"].tolist() == ["REPLACED"]

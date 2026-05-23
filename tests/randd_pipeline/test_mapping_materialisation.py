from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.assets.mapping import (
    load_ultfoc_mapper_from_csv,
    materialise_mapped_responses,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource


def test_load_ultfoc_mapper_from_csv_raises_for_missing_required_columns(tmp_path) -> None:
    csv_path = tmp_path / "ultfoc_missing.csv"
    pd.DataFrame({"ruref": ["A"]}).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="missing required columns: ultfoc"):
        load_ultfoc_mapper_from_csv(csv_path)


def test_materialise_mapped_responses_writes_to_intermediate_mapped_responses(tmp_path) -> None:
    resource = TableStoreResource(
        catalog_name="mapping-materialise-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    store = resource.get_table_store()

    expected = pd.DataFrame(
        {"reference": ["A"], "instance": [1], "survey_type": ["BERD"], "survey_year": [2024], "ultfoc": ["GB"]}
    )
    materialise_mapped_responses(store=store, df=expected, overwrite=False)

    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
    pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True), check_like=False)


def test_materialise_mapped_responses_overwrite_true_replaces_contents(tmp_path) -> None:
    resource = TableStoreResource(
        catalog_name="mapping-materialise-overwrite-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    store = resource.get_table_store()

    first = pd.DataFrame(
        {"reference": ["A"], "instance": [1], "survey_type": ["BERD"], "survey_year": [2024], "ultfoc": ["GB"]}
    )
    second = pd.DataFrame(
        {"reference": ["B"], "instance": [2], "survey_type": ["PNP"], "survey_year": [2025], "ultfoc": ["US"]}
    )

    materialise_mapped_responses(store=store, df=first, overwrite=False)
    materialise_mapped_responses(store=store, df=second, overwrite=True)

    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_MAPPED_RESPONSES)
    pd.testing.assert_frame_equal(actual.reset_index(drop=True), second.reset_index(drop=True), check_like=False)

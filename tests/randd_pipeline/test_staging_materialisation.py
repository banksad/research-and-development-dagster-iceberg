from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from src.randd_pipeline.assets.staging import (
    load_staging_contributors_from_csv,
    load_staging_responses_long_from_csv,
    materialise_staged_responses,
)
from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource


def test_load_staging_contributors_from_csv_raises_for_missing_required_columns(tmp_path) -> None:
    csv_path = tmp_path / "contributors_missing.csv"
    pd.DataFrame({"reference": ["A"], "instance": [1], "survey": ["BERD"]}).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="missing required columns: period"):
        load_staging_contributors_from_csv(csv_path)


def test_load_staging_responses_long_from_csv_raises_for_missing_required_columns(tmp_path) -> None:
    csv_path = tmp_path / "responses_missing.csv"
    pd.DataFrame(
        {
            "reference": ["A"],
            "instance": [1],
            "survey": ["BERD"],
            "period": [2024],
            "questioncode": [601],
        }
    ).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="missing required columns: response"):
        load_staging_responses_long_from_csv(csv_path)


def test_materialise_staged_responses_writes_to_intermediate_staged_responses(tmp_path) -> None:
    resource = TableStoreResource(
        catalog_name="staging-materialise-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    store = resource.get_table_store()

    expected = pd.DataFrame(
        {"reference": ["A"], "instance": [1], "survey_type": ["BERD"], "survey_year": [2024]}
    )

    materialise_staged_responses(store=store, df=expected, overwrite=False)

    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES)
    pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True), check_like=False)


def test_materialise_staged_responses_overwrite_true_replaces_contents(tmp_path) -> None:
    resource = TableStoreResource(
        catalog_name="staging-materialise-overwrite-smoke",
        catalog_type="local_sql",
        warehouse=str(tmp_path / "warehouse"),
    )
    store = resource.get_table_store()

    first = pd.DataFrame({"reference": ["A"], "instance": [1], "survey_type": ["BERD"], "survey_year": [2024]})
    second = pd.DataFrame({"reference": ["B"], "instance": [2], "survey_type": ["PNP"], "survey_year": [2025]})

    materialise_staged_responses(store=store, df=first, overwrite=False)
    materialise_staged_responses(store=store, df=second, overwrite=True)

    actual = store.read_table_as_dataframe(refs.INTERMEDIATE_STAGED_RESPONSES)
    pd.testing.assert_frame_equal(actual.reset_index(drop=True), second.reset_index(drop=True), check_like=False)

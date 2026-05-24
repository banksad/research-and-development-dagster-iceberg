from __future__ import annotations

import pytest

dagster = pytest.importorskip("dagster")
pd = pytest.importorskip("pandas")

from src.randd_pipeline.checks.asset_check_helpers import read_table_for_check
from src.randd_pipeline.resources import TableStoreResource


def _resource(tmp_path):
    return TableStoreResource(catalog_name="checks", catalog_type="local_sql", warehouse=str(tmp_path / "w"))


def test_existing_table_returns_dataframe_and_no_failure_result(tmp_path):
    resource = _resource(tmp_path)
    store = resource.get_table_store()
    df = pd.DataFrame({"a": [1]})
    table_id = "intermediate.test_table"
    store.create_table_from_dataframe(table_id, df, overwrite=False)

    returned_df, result = read_table_for_check(resource, table_id, check_label="helper check")
    assert result is None
    assert returned_df is not None
    assert returned_df.equals(df)


def test_missing_table_returns_failed_asset_check_result_with_message(tmp_path):
    resource = _resource(tmp_path)
    table_id = "intermediate.missing_table"

    returned_df, result = read_table_for_check(resource, table_id, check_label="required columns")
    assert returned_df is None
    assert isinstance(result, dagster.AssetCheckResult)
    assert not result.passed
    assert table_id in result.description
    assert "required columns" in result.description

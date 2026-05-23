import pandas as pd

from src.randd_pipeline.io import refs
from src.randd_pipeline.io.catalog_config import local_sql_catalog_config, rest_catalog_config
from src.randd_pipeline.io.iceberg import LocalPyIcebergTableStore, load_catalog



def _synthetic_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "survey_year": 2023,
                "survey_type": "BERD",
                "reference": 1001,
                "instance": 0,
                "value": 12.5,
            },
            {
                "survey_year": 2023,
                "survey_type": "BERD",
                "reference": 1002,
                "instance": 0,
                "value": 30.0,
            },
        ]
    )


def test_local_iceberg_store_create_append_read(tmp_path):
    config = local_sql_catalog_config(name="local-iceberg", warehouse_path=tmp_path)
    store = LocalPyIcebergTableStore(config)

    assert not store.table_exists("raw.unknown")

    store.ensure_namespace("raw")
    store.create_table_from_dataframe(refs.RAW_FULL_RESPONSES, _synthetic_rows())

    out = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES)
    assert len(out) == 2
    assert set(out.columns) == {"survey_year", "survey_type", "reference", "instance", "value"}

    store.append_dataframe(
        refs.RAW_FULL_RESPONSES,
        pd.DataFrame(
            [
                {
                    "survey_year": 2023,
                    "survey_type": "BERD",
                    "reference": 1003,
                    "instance": 0,
                    "value": 9.5,
                }
            ]
        ),
    )

    final_df = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES).sort_values("reference")
    assert len(final_df) == 3
    assert final_df["reference"].tolist() == [1001, 1002, 1003]


def test_rest_catalog_config_is_configuration_only():
    config = rest_catalog_config(name="future", uri="https://example.invalid/catalog")

    assert config.type == "rest"
    assert config.uri == "https://example.invalid/catalog"
    assert config.properties["uri"] == "https://example.invalid/catalog"


def test_load_catalog_rejects_unknown_catalog_type():
    class _Invalid:
        name = "x"
        type = "unknown"
        properties = {}

    try:
        load_catalog(_Invalid())  # type: ignore[arg-type]
    except ValueError as err:
        assert "Unsupported catalog config type" in str(err)
    else:
        raise AssertionError("Expected ValueError for unsupported catalog type")


def test_create_existing_table_with_overwrite_false_raises_value_error(tmp_path):
    config = local_sql_catalog_config(name="local-iceberg", warehouse_path=tmp_path)
    store = LocalPyIcebergTableStore(config)

    store.create_table_from_dataframe(refs.RAW_FULL_RESPONSES, _synthetic_rows())

    try:
        store.create_table_from_dataframe(refs.RAW_FULL_RESPONSES, _synthetic_rows(), overwrite=False)
    except ValueError as err:
        assert "already exists" in str(err)
    else:
        raise AssertionError("Expected ValueError when table exists and overwrite is False")


def test_create_existing_table_with_overwrite_true_replaces_contents(tmp_path):
    config = local_sql_catalog_config(name="local-iceberg", warehouse_path=tmp_path)
    store = LocalPyIcebergTableStore(config)

    store.create_table_from_dataframe(refs.RAW_FULL_RESPONSES, _synthetic_rows())

    replacement = pd.DataFrame(
        [
            {
                "survey_year": 2024,
                "survey_type": "BERD",
                "reference": 2001,
                "instance": 0,
                "value": 111.0,
            }
        ]
    )

    store.create_table_from_dataframe(refs.RAW_FULL_RESPONSES, replacement, overwrite=True)

    out = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES)
    assert len(out) == 1
    assert out["reference"].tolist() == [2001]
    assert out["value"].tolist() == [111.0]


def test_unknown_table_exists_returns_false(tmp_path):
    config = local_sql_catalog_config(name="local-iceberg", warehouse_path=tmp_path)
    store = LocalPyIcebergTableStore(config)

    assert store.table_exists("raw.not_a_real_table") is False

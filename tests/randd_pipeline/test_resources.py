import pandas as pd
import pytest

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource


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


def test_table_store_resource_local_sql_supports_create_append_read(tmp_path):
    resource = TableStoreResource(
        catalog_name="local-dev",
        catalog_type="local_sql",
        warehouse=str(tmp_path),
    )

    store = resource.get_table_store()
    assert hasattr(store, "create_table_from_dataframe")
    assert hasattr(store, "append_dataframe")
    assert hasattr(store, "read_table_as_dataframe")

    store.create_table_from_dataframe(refs.RAW_FULL_RESPONSES, _synthetic_rows())
    store.append_dataframe(refs.RAW_FULL_RESPONSES, _synthetic_rows().head(1))

    out = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES).sort_values("reference")
    assert len(out) == 3
    assert out["reference"].tolist() == [1001, 1001, 1002]


def test_table_store_resource_rest_raises_not_implemented_without_network_calls():
    resource = TableStoreResource(
        catalog_name="future-rest",
        catalog_type="rest",
        uri="https://example.invalid/catalog",
        properties={"auth-type": "none"},
    )

    try:
        resource.get_table_store()
    except NotImplementedError as err:
        assert "future implementation" in str(err)
        assert "REST/lakehouse-service" in str(err)
    else:
        raise AssertionError("Expected NotImplementedError for REST catalog type")




def test_table_store_resource_defaults_properties_to_empty_dict(tmp_path):
    resource = TableStoreResource(
        catalog_name="local-dev",
        catalog_type="local_sql",
        warehouse=str(tmp_path),
    )

    assert resource.properties == {}


def test_randd_pipeline_modules_import_without_production_config_or_endpoints():
    import importlib

    resources_mod = importlib.import_module("src.randd_pipeline.resources")
    definitions_mod = importlib.import_module("src.randd_pipeline.definitions")

    assert hasattr(resources_mod, "TableStoreResource")
    assert hasattr(definitions_mod, "defs")

def test_table_store_resource_local_sql_has_no_production_endpoint_or_credentials(tmp_path):
    resource = TableStoreResource(
        catalog_name="local-dev",
        catalog_type="local_sql",
        warehouse=str(tmp_path),
        properties={"warehouse": str(tmp_path)},
    )

    assert resource.uri is None
    assert "gcs" not in resource.warehouse.lower()
    assert "s3" not in resource.warehouse.lower()
    assert "hdfs" not in resource.warehouse.lower()
    assert "\\\\" not in resource.warehouse
    assert "credential" not in " ".join(resource.properties.keys()).lower()


def test_table_store_resource_local_sql_warehouse_only_normalises_properties(tmp_path):
    resource = TableStoreResource(
        catalog_name="local-dev",
        catalog_type="local_sql",
        warehouse=str(tmp_path),
    )

    store = resource.get_table_store()

    assert store._config.properties["warehouse"] == str(tmp_path)
    assert store._config.properties["uri"].startswith("sqlite:///")


def test_table_store_resource_local_sql_without_warehouse_raises_value_error():
    resource = TableStoreResource(
        catalog_name="local-dev",
        catalog_type="local_sql",
    )

    with pytest.raises(ValueError, match="local_sql catalog requires a warehouse path"):
        resource.get_table_store()


def test_table_store_resource_local_sql_can_create_and_read_without_properties(tmp_path):
    resource = TableStoreResource(
        catalog_name="local-dev",
        catalog_type="local_sql",
        warehouse=str(tmp_path),
    )

    store = resource.get_table_store()
    expected = _synthetic_rows()

    store.create_table_from_dataframe(refs.RAW_FULL_RESPONSES, expected)

    out = store.read_table_as_dataframe(refs.RAW_FULL_RESPONSES).sort_values("reference")
    assert out["reference"].tolist() == [1001, 1002]

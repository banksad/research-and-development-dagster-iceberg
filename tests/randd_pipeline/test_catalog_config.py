from src.randd_pipeline.io.catalog_config import (
    local_sql_catalog_config,
    rest_catalog_config,
)


def test_local_sql_catalog_config_uses_tmp_path(tmp_path):
    config = local_sql_catalog_config(name="local-test", warehouse_path=tmp_path)

    assert config.name == "local-test"
    assert config.type == "local_sql"
    assert config.warehouse == str(tmp_path)
    assert config.uri == f"sqlite:///{tmp_path / 'catalog.sqlite'}"
    assert config.properties["warehouse"] == str(tmp_path)
    assert config.properties["uri"].startswith("sqlite:///")


def test_rest_catalog_config_requires_no_credentials():
    config = rest_catalog_config(
        name="future-rest",
        uri="https://lakehouse.internal/catalog",
        warehouse="s3://logical-warehouse",
    )

    assert config.name == "future-rest"
    assert config.type == "rest"
    assert config.uri == "https://lakehouse.internal/catalog"
    assert config.warehouse == "s3://logical-warehouse"
    assert "token" not in config.properties

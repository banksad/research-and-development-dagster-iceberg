"""Import smoke tests for lean refoundation Dagster definitions."""

from src.randd_pipeline import definitions


def test_randd_pipeline_definitions_importable():
    assert hasattr(definitions, "defs")
    assert hasattr(definitions.defs, "assets")

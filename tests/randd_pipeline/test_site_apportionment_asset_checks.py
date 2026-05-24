from __future__ import annotations
import pytest
from src.randd_pipeline import definitions

def test_asset_checks_registered():
    pytest.importorskip("dagster")
    names={c.name for c in definitions.defs.asset_checks}
    assert "site_apportioned_responses_non_empty" in names
    assert "site_apportioned_responses_required_columns" in names

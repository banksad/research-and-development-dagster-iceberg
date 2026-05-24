from __future__ import annotations
import importlib
import pytest

pd = pytest.importorskip("pandas")

from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv
from src.randd_pipeline.domain.site_apportionment import apply_explicit_site_apportionment


def _inputs():
    return (load_scenario_csv("estimation_to_site_apportionment_minimal", "input_estimated_responses.csv"), load_scenario_csv("estimation_to_site_apportionment_minimal", "input_site_apportionment_factors.csv"))


def test_expected_and_structure_and_no_mutation():
    est, fac = _inputs(); e0, f0 = est.copy(deep=True), fac.copy(deep=True)
    expected = load_scenario_csv("estimation_to_site_apportionment_minimal", "expected_site_apportioned_responses.csv")
    out = apply_explicit_site_apportionment(est, fac, value_columns=["211"])
    assert_frame_equal_sorted(out, expected, ["reference", "site_id"])
    assert out[out.reference == "SYN001"].shape[0] > 1
    assert float(out[out.reference == "SYN002"]["site_proportion"].iloc[0]) == 1.0
    pd.testing.assert_frame_equal(est, e0); pd.testing.assert_frame_equal(fac, f0)


def test_validation_failures():
    est, fac = _inputs()
    bads = [
        (est.drop(columns=["instance"]), fac, {"value_columns": ["211"]}),
        (est, fac.drop(columns=["site_id"]), {"value_columns": ["211"]}),
        (est, fac, {"value_columns": []}),
        (est, fac, {"value_columns": ["missing"]}),
        (est, fac, {"value_columns": ["211"], "output_suffix": " "}),
        (est, fac, {"value_columns": ["211"], "factor_sum_tolerance": 0}),
    ]
    for e, f, kwargs in bads:
        with pytest.raises(ValueError): apply_explicit_site_apportionment(e, f, **kwargs)


def test_no_legacy_imports():
    importlib.import_module("src.randd_pipeline.domain.site_apportionment.explicit_factors")
    for forbidden in ["src.site_apportionment", "src.estimation", "src.outlier_detection", "src.imputation", "src.mapping", "src.pipeline", "src.staging", "freezing", "construction"]:
        assert forbidden not in importlib.sys.modules

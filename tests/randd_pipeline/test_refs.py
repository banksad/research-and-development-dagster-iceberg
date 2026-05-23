"""Tests for planned Iceberg table identifier contracts."""

from src.randd_pipeline.io import refs


def test_key_table_identifiers_exist():
    expected = {
        "raw.full_responses",
        "raw.backdata",
        "raw.manual_outliers",
        "raw.manual_trimming",
        "ref.postcode_mapper",
        "ref.pg_detailed_mapper",
        "ref.sic_division_detailed_mapper",
        "ops.response_corrections",
        "ops.postcode_corrections",
        "intermediate.staged_responses",
        "intermediate.mapped_responses",
        "intermediate.imputed_responses",
        "intermediate.outlier_adjusted_responses",
        "intermediate.estimated_responses",
        "intermediate.site_apportioned_responses",
        "mart.short_form",
        "mart.long_form",
        "mart.intram_totals",
    }

    assert expected.issubset(refs.PLANNED_TABLE_IDENTIFIERS)


def test_no_freezing_or_construction_identifiers_as_target_concepts():
    forbidden = ("freezing", "construction")
    for identifier in refs.PLANNED_TABLE_IDENTIFIERS:
        assert all(token not in identifier for token in forbidden)

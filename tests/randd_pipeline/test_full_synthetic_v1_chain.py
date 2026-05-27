from __future__ import annotations

import importlib

import pytest

pd = pytest.importorskip("pandas")
dagster = pytest.importorskip("dagster")

from src.randd_pipeline.io import refs
from src.randd_pipeline.resources import TableStoreResource
from tests.randd_pipeline.fixture_helpers import assert_frame_equal_sorted, load_scenario_csv, scenario_path

SCENARIO_ID = "full_pipeline_v1_minimal"

inputs_mod = importlib.import_module("src.randd_pipeline.assets.inputs")
staging_mod = importlib.import_module("src.randd_pipeline.assets.staging")
mapping_mod = importlib.import_module("src.randd_pipeline.assets.mapping")
imputation_mod = importlib.import_module("src.randd_pipeline.assets.imputation")
outliers_mod = importlib.import_module("src.randd_pipeline.assets.outliers")
estimation_mod = importlib.import_module("src.randd_pipeline.assets.estimation")
site_mod = importlib.import_module("src.randd_pipeline.assets.site_apportionment")
outputs_mod = importlib.import_module("src.randd_pipeline.assets.outputs")

staging_checks_mod = importlib.import_module("src.randd_pipeline.checks.staging_asset_checks")
mapping_checks_mod = importlib.import_module("src.randd_pipeline.checks.mapping_asset_checks")
imputation_checks_mod = importlib.import_module("src.randd_pipeline.checks.imputation_asset_checks")
outlier_checks_mod = importlib.import_module("src.randd_pipeline.checks.outlier_asset_checks")
estimation_checks_mod = importlib.import_module("src.randd_pipeline.checks.estimation_asset_checks")
site_checks_mod = importlib.import_module("src.randd_pipeline.checks.site_apportionment_asset_checks")
curated_checks_mod = importlib.import_module("src.randd_pipeline.checks.curated_statistics_asset_checks")
raw_checks_mod = importlib.import_module("src.randd_pipeline.checks.raw_input_checks")
ref_checks_mod = importlib.import_module("src.randd_pipeline.checks.ref_asset_checks")


def test_full_synthetic_v1_chain_materialises_once_and_checks_pass(tmp_path):
    fixture_dir = scenario_path(SCENARIO_ID)

    resource = TableStoreResource(catalog_name="full-chain", catalog_type="local_sql", warehouse=str(tmp_path / "warehouse"))
    defs = dagster.Definitions(
        assets=[
            inputs_mod.raw_full_responses,
            staging_mod.staged_responses,
            mapping_mod.ultfoc_mapper,
            mapping_mod.cell_number_mapper,
            mapping_mod.mapped_responses,
            imputation_mod.imputed_responses,
            outliers_mod.manual_outliers,
            outliers_mod.outlier_adjusted_responses,
            estimation_mod.estimated_responses,
            site_mod.site_apportionment_factors,
            site_mod.site_apportioned_responses,
            outputs_mod.curated_rnd_statistics,
        ],
        resources={"table_store": resource},
    )

    run_config = {
        "ops": {
            "raw__full_responses": {"config": {"csv_path": str(fixture_dir / "input_raw_full_responses.csv")}},
            "intermediate__staged_responses": {
                "config": {
                    "contributors_csv_path": str(fixture_dir / "input_staging_contributors.csv"),
                    "responses_long_csv_path": str(fixture_dir / "input_staging_responses_long.csv"),
                }
            },
            "ref__ultfoc_mapper": {"config": {"ultfoc_mapper_csv_path": str(fixture_dir / "input_ref_ultfoc_mapper.csv")}},
            "ref__cell_number_mapper": {"config": {"cell_number_mapper_csv_path": str(fixture_dir / "input_ref_cell_number_mapper.csv")}},
            "ops__manual_outliers": {"config": {"manual_outliers_csv_path": str(fixture_dir / "input_ops_manual_outliers.csv")}},
            "ref__site_apportionment_factors": {"config": {"site_factors_csv_path": str(fixture_dir / "input_ref_site_apportionment_factors.csv")}},
        }
    }

    run_result = defs.get_implicit_global_asset_job_def().execute_in_process(run_config=run_config)
    assert run_result.success

    store = resource.get_table_store()
    for table in [
        refs.RAW_FULL_RESPONSES,
        refs.REF_ULTFOC_MAPPER,
        refs.REF_CELL_NUMBER_MAPPER,
        refs.OPS_MANUAL_OUTLIERS,
        refs.REF_SITE_APPORTIONMENT_FACTORS,
        refs.INTERMEDIATE_STAGED_RESPONSES,
        refs.INTERMEDIATE_MAPPED_RESPONSES,
        refs.INTERMEDIATE_IMPUTED_RESPONSES,
        refs.INTERMEDIATE_OUTLIER_ADJUSTED_RESPONSES,
        refs.INTERMEDIATE_ESTIMATED_RESPONSES,
        refs.INTERMEDIATE_SITE_APPORTIONED_RESPONSES,
        refs.CURATED_RND_STATISTICS,
    ]:
        assert store.table_exists(table)

    imputed_df = store.read_table_as_dataframe(refs.INTERMEDIATE_IMPUTED_RESPONSES)
    for required_col in ["imp_class", "status", "601_imputed", "imp_marker"]:
        assert required_col in imputed_df.columns

    estimated_df = store.read_table_as_dataframe(refs.INTERMEDIATE_ESTIMATED_RESPONSES)
    for required_col in ["a_weight", "g_weight"]:
        assert required_col in estimated_df.columns
    assert (estimated_df["a_weight"] > 0).all()
    assert (estimated_df["g_weight"] > 0).all()

    for fn in [
        raw_checks_mod.raw_full_responses_table_exists,
        raw_checks_mod.raw_full_responses_non_empty,
        raw_checks_mod.raw_full_responses_required_columns,
        ref_checks_mod.ultfoc_mapper_table_exists,
        ref_checks_mod.ultfoc_mapper_non_empty,
        ref_checks_mod.ultfoc_mapper_required_columns,
        ref_checks_mod.ultfoc_mapper_unique_ruref,
        ref_checks_mod.cell_number_mapper_table_exists,
        ref_checks_mod.cell_number_mapper_non_empty,
        ref_checks_mod.cell_number_mapper_required_columns,
        ref_checks_mod.cell_number_mapper_unique_cellnumber,
        ref_checks_mod.cell_number_mapper_cellnumber_range,
        staging_checks_mod.staged_responses_table_exists,
        staging_checks_mod.staged_responses_non_empty,
        staging_checks_mod.staged_responses_required_columns,
        staging_checks_mod.staged_responses_unique_grain,
        mapping_checks_mod.mapped_responses_table_exists,
        mapping_checks_mod.mapped_responses_non_empty,
        mapping_checks_mod.mapped_responses_required_columns,
        mapping_checks_mod.mapped_responses_unique_grain,
        mapping_checks_mod.mapped_responses_ultfoc_present,
        mapping_checks_mod.mapped_responses_cell_number_mapping_complete,
        imputation_checks_mod.imputed_responses_table_exists,
        imputation_checks_mod.imputed_responses_non_empty,
        imputation_checks_mod.imputed_responses_required_columns,
        imputation_checks_mod.imputed_responses_unique_grain,
        imputation_checks_mod.imputed_responses_imputation_marker_populated,
        imputation_checks_mod.imputed_responses_no_illegal_missing_imputed_values,
        outlier_checks_mod.outlier_adjusted_responses_table_exists,
        outlier_checks_mod.outlier_adjusted_responses_non_empty,
        outlier_checks_mod.outlier_adjusted_responses_required_columns,
        outlier_checks_mod.outlier_adjusted_responses_unique_grain,
        outlier_checks_mod.outlier_adjusted_responses_outlier_flag_populated,
        outlier_checks_mod.outlier_adjusted_responses_outlier_source_populated,
        outlier_checks_mod.outlier_adjusted_responses_manual_adjustment_reason_present,
        estimation_checks_mod.estimated_responses_table_exists,
        estimation_checks_mod.estimated_responses_non_empty,
        estimation_checks_mod.estimated_responses_required_columns,
        estimation_checks_mod.estimated_responses_unique_grain,
        estimation_checks_mod.estimated_responses_weights_populated,
        estimation_checks_mod.estimated_responses_weights_positive,
        site_checks_mod.site_apportionment_factors_table_exists,
        site_checks_mod.site_apportionment_factors_required_columns,
        site_checks_mod.site_apportionment_factors_unique_site_grain,
        site_checks_mod.site_apportionment_factors_site_identifier_populated,
        site_checks_mod.site_apportionment_factors_proportions_sum_to_one,
        site_checks_mod.site_apportioned_responses_table_exists,
        site_checks_mod.site_apportioned_responses_non_empty,
        site_checks_mod.site_apportioned_responses_required_columns,
        site_checks_mod.site_apportioned_responses_unique_site_grain,
        site_checks_mod.site_apportioned_responses_site_identifier_populated,
        site_checks_mod.site_apportioned_responses_site_proportion_populated,
        site_checks_mod.site_apportioned_responses_apportioned_values_non_negative,
        curated_checks_mod.curated_rnd_statistics_table_exists,
        curated_checks_mod.curated_rnd_statistics_non_empty,
        curated_checks_mod.curated_rnd_statistics_required_columns,
        curated_checks_mod.curated_rnd_statistics_unique_grain,
        curated_checks_mod.curated_rnd_statistics_output_measure_populated,
        curated_checks_mod.curated_rnd_statistics_output_value_non_negative,
        curated_checks_mod.curated_rnd_statistics_provenance_columns_present,
        curated_checks_mod.curated_rnd_statistics_reconciles_to_site_input,
    ]:
        assert fn(resource).passed

    assert_frame_equal_sorted(
        store.read_table_as_dataframe(refs.CURATED_RND_STATISTICS),
        load_scenario_csv(SCENARIO_ID, "expected_curated_rnd_statistics.csv"),
        ["survey_year", "survey_type", "output_measure"],
    )


def test_full_chain_runtime_isolation_from_legacy_modules():
    forbidden = [
        "src.outputs",
        "src.site_apportionment",
        "src.estimation",
        "src.outlier_detection",
        "src.imputation",
        "src.mapping",
        "src.pipeline",
        "src.staging",
        "freezing",
        "construction",
    ]
    for mod in [
        "src.randd_pipeline.assets.staging",
        "src.randd_pipeline.assets.mapping",
        "src.randd_pipeline.assets.imputation",
        "src.randd_pipeline.assets.outliers",
        "src.randd_pipeline.assets.estimation",
        "src.randd_pipeline.assets.site_apportionment",
        "src.randd_pipeline.assets.outputs",
    ]:
        importlib.import_module(mod)

    for prefix in forbidden:
        assert not any(name == prefix or name.startswith(f"{prefix}.") for name in importlib.sys.modules)

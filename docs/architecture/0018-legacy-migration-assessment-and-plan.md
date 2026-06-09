# 0018: Refoundation assessment and legacy migration plan

- **Status:** Proposed
- **Date:** 2026-06-09

## Purpose

Assess whether the `src/randd_pipeline` Dagster + Iceberg refoundation is heading in the right direction, and set out a phased plan for migrating the remainder of the legacy pipeline (`src/pipeline.py` and the stage modules under `src/`) into the refoundation. This document builds on ADR 0002 (migration principles), ADR 0004 (legacy deprecation map), and the seam inventories (ADRs 0005–0017).

## Part 1: Assessment

### Verdict

The refoundation is heading in the right direction. The v1 vertical slice establishes sound, repeatable patterns that directly serve the reproducibility and auditability goals, and the remaining work is primarily breadth (porting legacy stages into those patterns) plus a small number of cross-cutting foundations.

### Strengths to preserve

1. **Clean layering.** Pure, Dagster-agnostic domain functions (`src/randd_pipeline/domain/<seam>/`) sit behind thin Dagster assets (`src/randd_pipeline/assets/`), with per-seam asset checks (`src/randd_pipeline/checks/`) and Iceberg persistence behind the `TableStore` protocol (`src/randd_pipeline/io/table_store.py`). Business logic is testable without Dagster installed.
2. **Auditable lakehouse layout.** The `raw` / `ref` / `ops` / `intermediate` / `curated` / `mart` namespace plan in `src/randd_pipeline/io/refs.py` gives every dataset a clear layer, owner, and contract surface (`src/randd_pipeline/io/contracts.py`).
3. **Synthetic-fixture-first testing.** Tiny, deterministic, human-readable scenarios under `tests/fixtures/synthetic/scenarios/` plus seam, materialisation, check, and chain tests (55 test files) mean no production data is needed locally.
4. **Auditability built in.** Provenance markers (imputation `imp_marker`, outlier source tracking) and 48 asset checks captured per materialisation directly support audit of statistical decisions.
5. **Deliberate process.** ADRs 0001–0017 and `docs/developer/refoundation-dev-setup.md` document intent, seam inventories, and naming conventions — migration can proceed against written decisions rather than tribal knowledge.

### Gaps and risks

Each item maps to a foundation work item or migration phase in Part 2.

1. **No partitioning or incremental strategy.** All assets do full-table rewrites; no Iceberg partition specs exist, and the table contracts carry no `partition_by` values. Production volumes need partitioning by `survey_year` / `survey_type` (Phase 0).
2. **Production catalog path unproven.** The `rest` branch of `src/randd_pipeline/io/catalog_config.py` raises `NotImplementedError`; all development runs on the SQLite-backed local catalog. Commit/overwrite semantics may differ between catalogs and should be exercised early (Phase 0 mitigation, Phase 10 implementation).
3. **Provenance not populated.** `source_snapshot_id` exists in the curated output config but is never populated; Iceberg snapshot IDs are not recorded against Dagster runs (Phase 0/1).
4. **Mixed configuration styles.** Older assets use `config_schema` dicts while newer ones use Pydantic `Config` classes; standardise on Pydantic (Phase 0).
5. **No parity harness exists yet.** `docs/developer/refoundation-dev-setup.md` describes the intent, but no comparison code exists in `helpers/` or `tests/`. ADR 0002 makes parity-first migration a principle, so this is the single most important missing piece (Phase 0).
6. **Thin functional slice.** Missing relative to legacy: SPP JSON snapshot ingestion, freezing, postcode/ITL/product-group/SIC mapping, Northern Ireland, construction, Mean-of-Ratios imputation with backdata, short-form expansion, automatic outlier detection, full estimation weight application, postcode-based site apportionment, and the 16-output dissemination layer (Phases 1–9).
7. **Housekeeping.** The debug asset `cell_number_mapped_responses` lingers outside default definitions; no jobs, schedules, or sensors are defined; table contracts are loaded but not enforced by checks.

## Part 2: Migration plan

### Guiding decisions

- **D1 — BERD/PNP branching: multi-partitions over one graph.** Use a `MultiPartitionsDefinition` of `survey_year` (static, extendable) × `survey_type` (`BERD`, `PNP`) over a single linear asset graph. For PNP partitions, the outlier and estimation domain functions return their input unchanged with marker columns (mirroring `src/pipeline.py`, where PNP skips both stages), and short-form expansion is skipped inside the imputation seam. Avoid conditional asset subsets: they fragment lineage, checks, and contracts.
- **D2 — Freezing: snapshot tags plus an explicit amendment workflow.** Do not port `src/freezing/` wholesale. Iceberg snapshot tags (for example `frozen-{survey_year}`) and time travel subsume frozen-CSV persistence, per ADR 0004 item 1. What must be ported is the analyst compare-and-amend workflow: a diff of a fresh SPP snapshot against the tagged frozen state, surfaced as `ops.freezing_additions` / `ops.freezing_amendments` tables with an accept-changes column, plus `freeze_comparison_job` and `apply_freeze_job` Dagster jobs replacing the legacy early-exit run-mode flags. This is the one intentional behaviour divergence from legacy and needs its own ADR and stakeholder sign-off rather than blind parity.
- **D3 — Outputs: mart tables as system of record.** Each legacy output under `src/outputs/` becomes a `mart.*` Iceberg table. CSV/Excel filename templating is isolated to optional terminal export assets (`src/randd_pipeline/assets/exports.py`), excluded from default production definitions, per ADR 0004 item 4.
- **D4 — Parity: golden-output harness over shared synthetic scenarios.** Run legacy `run_pipeline` and the Dagster job on the same synthetic scenario and compare legacy stage QA CSVs (enabled via the legacy `output_*_qa` config flags) against the corresponding Iceberg tables after normalisation (sorting, dtype coercion, NaN handling, numeric tolerance). This gives stage-level diffing, not just end-to-end.

### Phase 0 — Cross-cutting foundations (effort: L)

Prerequisite for all later phases.

1. **Parity harness** (new): `tests/parity/harness.py` with `run_legacy_pipeline(scenario_dir, tmp_path)` (drives `src/pipeline.py` with generated user/dev YAML pointing at the scenario), `run_refoundation_job(scenario_dir, tmp_warehouse)`, and `compare_frames(legacy_df, new_df, key_cols, tolerance)`. Per-stage `tests/parity/test_parity_<stage>.py`, marked as slow/integration with a dedicated make target. One shared rich scenario per survey type: `tests/fixtures/synthetic/scenarios/parity_berd_full/` and later `parity_pnp_full/`, with SPP-shaped JSON, all mappers, backdata, and manual files, consumable by both pipelines.
2. **Partitioning and incremental writes**: extend the `TableStore` protocol and `LocalPyIcebergTableStore` with partition-scoped overwrite; add `src/randd_pipeline/partitions.py` with the D1 multi-partition definition; retrofit existing v1 assets to be partition-aware.
3. **Config consolidation**: migrate remaining `config_schema` dict assets to Pydantic `Config` classes; add a shared run-scope config (`survey_year`, `survey_type`) derived from partition keys.
4. **Provenance**: add `ops.run_provenance` to `io/refs.py`; record Dagster `run_id` and Iceberg snapshot IDs in materialisation metadata (replaces `src/utils/runlog.py`, per ADR 0004 item 3); populate `source_snapshot_id` from Phase 1 onwards.
5. **Housekeeping**: remove the `cell_number_mapped_responses` debug asset from default definitions; begin enforcing table contracts in checks.
6. **Catalog risk mitigation**: stand up a local REST catalog container in CI so SQLite-vs-REST behavioural differences surface early.

Risk: the partition retrofit touches every existing asset and test; do it before feature migration multiplies the surface.

### Phase 1 — Real staging and SPP ingestion (effort: L)

- **Legacy ported:** `src/staging/` (`spp_parser.py`, `spp_snapshot_processing.py`, `validation.py`, `postcode_validation.py`, `staging_helpers.py`, `staging_main.py`).
- **New seams:** `domain/staging/spp_snapshot.py` (JSON → contributors/responses → wide full responses), `domain/staging/validation.py`, `domain/staging/postcode_validation.py`, `domain/staging/reference_loads.py`.
- **New assets/tables:** real SPP snapshot ingestion behind `raw.full_responses` (or a new `raw.spp_snapshot`); implement the planned `ref.postcode_mapper`, `ref.pg_detailed_mapper`, `ref.sic_division_detailed_mapper`, `raw.backdata`, `raw.manual_trimming` identifiers; port legacy schema validation rules as asset checks.
- **Parity:** `intermediate.staged_responses` versus the legacy staging QA output on `parity_berd_full`.
- **Why first:** every downstream phase needs realistically shaped data, and this unlocks `source_snapshot_id` provenance. The current v1 fixtures are too idealised to expose parity bugs.

### Phase 2 — Mapping completion (effort: M)

- **Legacy ported:** `src/mapping/itl_mapping.py`, `pg_conversion.py`, `cellno_mapping.py` (full rules), `pnp_mapping.py`, `mapping_helpers.py` (NI mapping deferred to Phase 7).
- **New seams:** `domain/mapping/itl.py`, `domain/mapping/product_group.py`, `domain/mapping/pnp.py`; extend `domain/mapping/cell_number.py`. Compose into the canonical `intermediate.mapped_responses` per ADR 0009/0011.
- **New tables:** `ref.itl_mapper`, `ref.pg_numeric_alpha_mapper`.
- **Checks:** postcode-mapped coverage, ITL populated, product-group alpha validity.
- **Parity:** `intermediate.mapped_responses` versus legacy mapping QA output.

### Phase 3 — Full imputation (effort: XL — highest risk)

- **Legacy ported:** all of `src/imputation/` (`apportionment.py`, `short_to_long.py`, `MoR.py`, `tmi_imputation.py` replacing `domain/imputation/simple_tmi.py`, `sf_expansion.py`, `manual_imputation.py`, `impute_civ_def.py`, `imputation_helpers.py`) plus `src/utils/breakdown_validation.py` as asset checks.
- **New seams:** `domain/imputation/{apportionment,short_to_long,mean_of_ratios,tmi,sf_expansion,manual_trimming,civil_defence}.py`.
- **New tables:** keep one canonical `intermediate.imputed_responses` plus QA tables `ops.imputation_links_qa` and `ops.imputation_trim_qa` mirroring the legacy QA CSVs (these double as parity surfaces), and `curated.backdata_next_year` (legacy backdata output for the following cycle).
- **Checks:** breakdown validation (components sum to totals); `imp_marker` domain check (`TMI`/`MoR`/`CF`/`constructed`/`no_imputation`).
- **Parity:** legacy imputed QA, links QA, and trimming QA CSVs versus the new tables, for BERD (with short-form expansion) and PNP (without).
- **Risks:** trimmed-mean tie-breaking, pandas sort stability, float accumulation in Mean-of-Ratios links, backdata join semantics, and special-case overwrite rules. Mitigate with numeric tolerance plus row-level diff reporting in the harness.

### Phase 4 — Automatic outliers and full estimation (effort: M)

- **Legacy ported:** `src/outlier_detection/` (auto-flagging composed with manual flags) and `src/estimation/` (apply a/g weights across the full variable set).
- **New seams:** `domain/outliers/auto_flagging.py`; extend `domain/estimation/weights.py`. PNP partitions are identity pass-through per D1.
- **New tables:** `ops.outlier_qa`.
- **Parity:** legacy outlier QA and estimated outputs versus `intermediate.outlier_adjusted_responses` / `intermediate.estimated_responses` (BERD); PNP pass-through equality check.

### Phase 5 — Full site apportionment (effort: M)

- **Legacy ported:** `src/site_apportionment/` (postcode inference, marker filtering, intramural totals).
- **New seams:** `domain/site_apportionment/postcode_inference.py`, `domain/site_apportionment/marker_filtering.py`, replacing the explicit-factors-only seam.
- **New tables:** materialise the planned `mart.intram_totals` here, since legacy computes intramural totals at this stage.
- **Checks:** site weights sum to one per reference; intramural total reconciliation.

### Phase 6 — Construction as explicit correction assets (effort: M)

- **Legacy ported:** `src/construction/`, reimagined per ADR 0002 principle 6 and ADR 0004 item 2 rather than transliterated.
- **New seams:** `domain/corrections/apply_response_corrections.py`, `domain/corrections/apply_postcode_corrections.py`.
- **New assets/tables:** implement the planned `ops.response_corrections` and `ops.postcode_corrections`; insert `intermediate.corrected_responses` between staged and mapped (legacy all-data construction), and a post-imputation postcode-correction step feeding outliers, including updated-postcode validation — mirroring the legacy graph, where construction runs twice.
- **Checks:** every correction row matched a target record; correction audit columns populated.
- **Topology note:** this re-points the dependencies of `mapped_responses`, so do it after Phases 2–5 stabilise; empty correction tables must behave as identity so existing parity scenarios are unaffected.

### Phase 7 — Northern Ireland (effort: M, parallelisable)

- **Legacy ported:** `src/northern_ireland/` and `src/mapping/ni_mapping.py` (NI SAS output lands in Phase 9).
- **New seams/assets:** `domain/northern_ireland/staging.py`, `domain/mapping/ni.py`; `raw.ni_responses`, `intermediate.ni_mapped_responses`. NI is a side-chain joining at outputs, so it can be developed in parallel; model the legacy `load_ni_data` flag as Dagster asset selection rather than config branching.

### Phase 8 — Freezing replacement (effort: M, high conceptual risk)

Per D2. Legacy `src/freezing/` is referenced, not transliterated.

- **New seams/assets:** `domain/freezing/compare.py` (diff fresh snapshot versus tagged frozen state into additions/amendments), `domain/freezing/apply.py` (merge approved amendments and re-tag); `ops.freezing_additions`, `ops.freezing_amendments`; `tag_snapshot` / tag-read support on `TableStore` and `LocalPyIcebergTableStore`; `freeze_comparison_job` and `apply_freeze_job` in `definitions.py`.
- **Governance:** a dedicated ADR for freezing-as-Iceberg-snapshots with stakeholder sign-off, since behaviour intentionally diverges from legacy mechanics. Parity is asserted on the additions/amendments content for the same before/after snapshot pair, not on the mechanism.

### Phase 9 — Outputs and dissemination marts (effort: XL, mostly mechanical)

- **Legacy ported:** `src/outputs/`, in three waves:
  - **9a — parity-critical core:** `form_output_prep.py`, `long_form.py`, `short_form.py`, `tau.py`, `gb_sas.py`, `intram_totals.py`, `map_output_cols.py`, `outputs_helpers.py` → `domain/outputs/` seams feeding `mart.long_form`, `mart.short_form` (already planned), plus new `mart.tau` and `mart.gb_sas`. `curated.rnd_statistics` is redefined atop the shared form-output-prep seam.
  - **9b — breakdowns and remainder:** `intram_by_pg.py`, `intram_by_itl.py`, `intram_by_sic.py`, `intram_by_civil_defence.py`, `frozen_group.py`, `total_fte.py`, `ni_sas.py`, `PNP_NA_output.py` → corresponding `mart.*` identifiers in `io/refs.py`.
  - **9c — exports:** `export_files.py`, `manifest_output.py` → terminal CSV/Excel export assets in `assets/exports.py`, holding all filename templating, excluded from default production definitions.
- **Checks:** column contracts per mart; cross-mart reconciliation (each intramural breakdown sums to `mart.intram_totals`).
- **Parity:** the strongest overall signal — normalised comparison of all 16 legacy output CSVs against mart tables on `parity_berd_full` and `parity_pnp_full`. Legacy `output_*` config flags map to Dagster asset selection.

### Phase 10 — Production hardening and legacy decommission (effort: L)

1. **REST catalog:** implement the `rest` branch of `io/catalog_config.py` (PyIceberg `load_catalog` parametrisation) and wire a `rest` mode into `TableStoreResource`, with env-var/secret-based credentials (none committed).
2. **Jobs, schedules, sensors:** `full_pipeline_job` (per partition), the two freeze jobs, a sensor watching the SPP snapshot landing location, and an annual-cycle schedule.
3. **Decommission**, gated per ADR 0004 deletion gates (parity green, rollback documented, no production dependency): staged deletion of `src/pipeline.py`, `main.py`, the legacy stage modules (`src/staging`, `src/freezing`, `src/northern_ireland`, `src/construction`, `src/mapping`, `src/imputation`, `src/outlier_detection`, `src/estimation`, `src/site_apportionment`, `src/outputs`), legacy plumbing (`src/utils/{runlog,local_file_mods,s3_mods,hdfs_mods,singleton_boto,path_helpers}.py`), the dual YAML configs, legacy orchestration scaffolding under `src/orchestration/`, and the 59 legacy test modules — one stage at a time as each phase's parity locks in. Retire the parity harness last.

### Sequencing and effort summary

| Phase | Scope | Effort | Risk |
|---|---|---|---|
| 0 | Foundations: parity harness, partitions, config, provenance | L | Medium — touches all existing assets |
| 1 | Real staging / SPP ingestion | L | Medium |
| 2 | Mapping completion (ITL, PG, cell number, PNP) | M | Low |
| 3 | Full imputation (MoR, TMI, sf_expansion, apportionment) | XL | Highest — numeric parity |
| 4 | Auto outliers + full estimation | M | Low–Medium |
| 5 | Full site apportionment | M | Medium |
| 6 | Construction → correction assets | M | Medium — graph topology change |
| 7 | Northern Ireland | M | Low — parallelisable |
| 8 | Freezing → snapshot tags + amendment workflow | M | High conceptual — intentional divergence |
| 9 | Outputs → mart tables + exports | XL | Low–Medium — wide but mechanical |
| 10 | REST catalog, jobs/schedules, decommission | L | Medium — production unknowns |

**Sequencing rationale:** Phases 0–1 come first because realistic SPP-shaped data and a working parity harness are prerequisites for trustworthy validation of everything downstream. Imputation (Phase 3) is the highest-risk numeric core, so it is tackled early-middle with the harness in place. The topology-changing construction phase waits until the main chain is stable. Outputs come last among features because they consume everything upstream. Decommission proceeds stage by stage only as parity locks in.

**Riskiest items overall:**

1. Imputation numeric parity (trim tie-breaks, float accumulation, sort-order-dependent results).
2. Freezing semantics replacement — the only intentional behaviour change; requires ADR and sign-off.
3. The Phase 0 partition retrofit of existing assets.
4. The REST catalog being entirely unimplemented while all development runs on SQLite — behavioural differences in commit/overwrite semantics may surface late; mitigate with a REST catalog container in CI from Phase 0.

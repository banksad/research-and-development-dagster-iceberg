# ADR 0017: Curated output Iceberg seam inventory and first clean implementation plan

- **Status:** Proposed
- **Date:** 2026-05-24

## 1. Purpose

This ADR provides a focused inventory and implementation plan for the **first clean curated output seam** after the production-facing refoundation chain now reaches:

- `intermediate.site_apportioned_responses`

This is a planning/inventory ADR only.

This ADR is explicitly:

- **not** a migration of legacy CSV/Excel output runners;
- **not** a wrapper around legacy orchestration;
- **not** a runtime implementation PR;
- **not** introducing assets/checks/fixtures/runtime code changes.

Legacy output files are treated as **parity/reference artefacts only** during migration. The target production input is canonical `intermediate.site_apportioned_responses`, and the target canonical output is an Iceberg-backed curated table (not CSV/Excel files).

## 2. Target output philosophy

The lean refoundation output model is:

- canonical statistical outputs should be Iceberg tables;
- CSV/Excel outputs are optional downstream presentation/export artefacts;
- future APIs should read curated output tables (or controlled serving views) rather than become canonical storage;
- reproducibility should be anchored by source/output snapshots, run config, checks, and provenance metadata.

Conceptual layering:

- **Intermediate layer**
  - `intermediate.site_apportioned_responses`
- **Curated statistical output layer**
  - `curated.rnd_statistics` (preferred first seam), later possibly `mart.short_form` / `mart.long_form`
- **Presentation/export layer**
  - CSV, Excel, API, dashboards

This PR plans only the curated table layer.

## 3. Current legacy role (conceptual)

### 3.1 What legacy output/finalisation currently consumes

The output stage receives:

- weighted/apportioned response-like microdata (`weighted_df`/`outputs_df`/`tau_outputs_df`);
- NI response data when enabled;
- output schemas from TOML;
- configuration booleans selecting which outputs run;
- path configuration for output and export locations.

### 3.2 What it currently produces

Legacy output modules primarily write CSV artefacts into output folders, including:

- short form and long form microdata extracts;
- TAU, GB SAS, NI SAS outputs;
- intramural cuts by PG, ITL, SIC, civil/defence;
- frozen group, total FTE QA, intramural totals;
- PNP National Accounts output (PNP-only);
- export manifest plus optional copy/move of selected files to an outgoing folder.

### 3.3 Grain and transformations

Legacy output logic mixes:

- response-level filtering and branch logic (e.g., form type/status, no-R&D filters);
- derived columns (sizeband/CORA status/headcount splits/composite spend columns);
- aggregation for specific publication views (e.g., intram totals, grouped intram outputs);
- schema projection to output-specific file layouts.

Conceptually, it transforms from response/site grain to multiple publication/output grains (including grouped/dimensioned aggregates).

### 3.4 Dimensions and measure behaviour found in legacy outputs

Dimensions and grouping fields appearing across modules include survey period/year, form/survey type branching, product group, ITL geography, SIC, civil/defence split, region (GB/UK), sizeband and status mapping fields, plus output-specific descriptor columns.

Measures include direct and derived numeric values from questionnaire codes (notably intramural `211` and composites like `C_lnd_bl`, `ovss_oth`, `oth_sc`), sometimes weighted and sometimes unweighted depending on output branch.

### 3.5 Config and side effects

Legacy output behaviour is heavily config-driven via many `global.output_*` toggles, schema paths, output paths, export selections, and platform file modules. Main side effects are CSV writes and export copy/move operations.

## 4. Function-level inventory

| File path | Function(s) | Current role | Inputs | Outputs | Side effects | Validation rules | Config/I-O deps | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|
| `src/pipeline.py` | `run_pipeline` (outputs call site) | Legacy orchestration wiring into outputs stage | full pipeline state | invokes outputs | stage sequencing/logging | implicit stage ordering | heavy config + file-platform mods | orchestration | keep legacy only |
| `src/outputs/outputs_main.py` | `run_outputs` | Central output dispatcher across output families | weighted df, NI df, intram dict, config | many output files | multiple CSV writes | branch guards via config | `global.output_*`, paths | mixed orchestration + I/O | future split: thin asset orchestration + domain fns; keep legacy now |
| `src/outputs/form_output_prep.py` | `filter_outputs`, `form_output_prep` | Prepares base output datasets, applies filters/weights | weighted df, NI df, config | `outputs_df`, `tau_outputs_df` | none direct | `flag_no_rand_spenders` rules | estimation + validation helpers | mixed business prep | extract/reshape into `src/randd_pipeline/domain/outputs/` |
| `src/outputs/short_form.py` | `create_headcount_cols`, `run_shortform_prep`, `output_short_form` | Short-form prep + schema projection + write | outputs df, config | short-form CSV | CSV write | form/status/instance filters | schema paths + output paths | mixed transform + file I/O | domain prep logic extract; CSV writing defer to export layer |
| `src/outputs/long_form.py` | `output_long_form` | Long-form projection and write | outputs df, config | long-form CSV | CSV write | formtype filtering | schema/output paths | mixed | treat as downstream export/parity view |
| `src/outputs/tau.py`, `src/outputs/gb_sas.py` | `output_tau`, `output_gb_sas` | Derived columns, mapping, intram totals + file outputs | prepared output dfs, config | TAU/GB SAS CSV + intram totals dict updates | CSV writes | implicit numeric assumptions | schema/output paths | mixed aggregation + I/O | extract reusable aggregation logic only |
| `src/outputs/intram_*` modules | `output_intram_by_pg`, `output_intram_by_itl`, `output_intram_by_sic`, `output_intram_by_civil_defence`, `output_intram_totals` | Publication-style grouped intram outputs and QA total writes | output dfs + detail maps + config | grouped CSVs, totals CSV | CSV writes | output-specific assumptions | schema/output paths | mixed aggregation + I/O | likely mart/export layer later |
| `src/outputs/ni_sas.py`, `src/outputs/frozen_group.py`, `src/outputs/total_fte.py`, `src/outputs/PNP_NA_output.py` | output writers | Specialized output families/QA files | output dfs + config | CSV outputs | CSV writes | branch-specific assumptions | schema/output paths + survey type | mixed | defer as downstream parity/export outputs |
| `src/outputs/export_files.py` | `run_export` and helpers | Post-output file transfer + manifest/export logging | config, selected output files | moved/copied files + manifest | file copy/move logging | file existence checks | platform/path/export config | file plumbing | deprecate from canonical runtime; keep legacy only |
| `src/site_apportionment/output_status_filtered.py` | marker filter QA helpers + output | Pre-output QA file on filtered markers | apportionment df/config | status-filtered CSV | CSV write | marker filter conventions | output paths/toggles | QA file I/O | replace with asset checks/QA tables later |

## 5. Business logic vs legacy plumbing classification

### 5.1 Keep/extract for curated domain logic

- minimal additive aggregations on apportioned value columns (e.g., `211_apportioned`-style measures);
- explicit grouping over a narrow, documented dimension set (`survey_year`, `survey_type`, optional small number of dimensions);
- reproducible measure-labelling logic (`output_measure`, `output_value`).

### 5.2 Rewrite as clean domain functions

- isolate pandas aggregation from schema projection and file writing;
- make grouping dimensions and allowed measure columns explicit in function signatures;
- produce contract-shaped DataFrames independent of file paths/toggles.

### 5.3 Replace with Dagster assets/checks + Iceberg tables

- replace legacy `run_outputs` branching for canonical output with one curated asset path;
- add output quality and reconciliation checks as Dagster asset checks;
- materialise curated tables in Iceberg (`curated.*` first, `mart.*` later if needed).

### 5.4 Defer to optional export layer

- short-form/long-form publication extracts;
- CSV/Excel writers;
- export file transfer/manifest plumbing.

### 5.5 Deprecate/delete legacy plumbing over time

- path-based output routing in canonical flow;
- many `output_*` toggles that only control file side effects;
- output copy/move workflows as primary production outputs.

## 6. Recommended first clean curated output seam

## Recommendation: `curated.rnd_statistics` (minimal v1)

Recommended next implementation seam:

- consume canonical `intermediate.site_apportioned_responses`;
- aggregate one (or at most two) apportioned numeric columns into long-format measures;
- keep dimensions minimal (`survey_year`, `survey_type`, optional one extra dimension only if unambiguous);
- write exactly one curated Iceberg table;
- treat legacy short/long/Excel/CSV outputs as downstream parity references only.

Why this seam first:

1. It is the smallest meaningful canonical statistical output contract.
2. It avoids premature commitment to publication-oriented long/short form shapes.
3. It proves reproducible Iceberg-first output semantics without legacy runner coupling.
4. It is easy to validate with tiny synthetic data and simple reconciliation checks.

If inventory ambiguity remains high around production publication grain, use this same table name with explicitly narrow v1 scope rather than introducing legacy wrapper seams.

## 7. Proposed table contract (documented only)

Proposed table (future implementation):

- **Identifier:** `curated.rnd_statistics`
- **Layer/namespace:** curated
- **Expected grain (v1):** one row per `survey_year + survey_type + output_measure` (unless one explicitly justified extra dimension is added)
- **Partitioning (initial proposal):** `survey_year` (consider optional `survey_type` secondary partition)

Proposed required columns (v1):

- `survey_year` (int/string per existing convention)
- `survey_type` (e.g., BERD/PNP)
- `output_measure` (e.g., `total_211_apportioned`)
- `output_value` (numeric)
- `source_table_identifier` (e.g., `intermediate.site_apportioned_responses`)
- `source_snapshot_id` (nullable placeholder for now)
- `pipeline_run_id` (nullable placeholder for now)
- `created_at` or equivalent materialisation timestamp (if standard in contracts)

Notes:

- do **not** update `config/table_contracts/base.yaml` in this planning PR;
- this ADR only records the proposed future contract shape.

## 8. Synthetic fixture requirements (future, not implemented)

Proposed future scenario directory:

- `tests/fixtures/synthetic/scenarios/site_apportionment_to_curated_output_minimal/`

Proposed files:

- input: `intermediate__site_apportioned_responses.csv`
- expected: `curated__rnd_statistics.csv`

Suggested minimal cases:

1. two site-apportioned rows aggregate into one curated output row;
2. one row remains separate due to differing dimension value;
3. one measure column only in v1 (e.g., `211_apportioned` -> `total_211_apportioned`);
4. preserve `survey_year` and `survey_type`;
5. include deterministic expected totals for reconciliation.

Why this fixture is suitable:

- tiny and deterministic;
- no sensitive/production data;
- covers aggregation and grain behaviour with minimal complexity.

## 9. Target asset/check rollout (future sequence)

1. Add domain aggregation function in `src/randd_pipeline/domain/outputs/`.
2. Add curated output asset in `src/randd_pipeline/assets/` consuming `intermediate.site_apportioned_responses`.
3. Add/activate table contract entry for `curated.rnd_statistics`.
4. Add initial asset checks in `src/randd_pipeline/checks/`.
5. Add chain smoke test from site apportionment fixture to curated output.
6. Optionally add materialisation metadata counters (row count, measure count, grouping cardinality).

## 10. Operator-facing check ideas (plain language)

- **Curated R&D Statistics - Required Columns Present**
- **Curated R&D Statistics - Output Not Empty**
- **Curated R&D Statistics - Unique Grain Rows**
- **Curated R&D Statistics - No Missing Survey Year/Type**
- **Curated R&D Statistics - Output Values Non-Negative**
- **Curated R&D Statistics - Input/Output Totals Reconcile** (within tolerance)
- **Curated R&D Statistics - Output Dimensions Populated**
- **Curated R&D Statistics - Provenance Metadata Present** (where applicable in stage maturity)

## 11. Dagster-native design considerations (ADR 0014 alignment)

- **Dagster Config:** operator-run scope only (e.g., survey year/type selection, run mode), not canonical table identity.
- **Resources:** Iceberg/catalog/IO manager and standard metadata utilities; avoid embedding business logic in resources.
- **Asset checks:** schema, grain uniqueness, nullness, non-negativity, and reconciliation checks.
- **Materialisation metadata:** source table identifier, source snapshot placeholder, run id placeholder, row counts, measure totals.
- **Blocking candidates later:** missing required columns, grain duplication, failed reconciliation beyond tolerance.
- **Partitioning:** `survey_year` is likely first partition key; assess whether `survey_type` partition materially improves access patterns.
- **Output selection:** canonical curated asset definitions should be fixed; operator config should scope partitions/runs, not switch canonical outputs on/off like legacy file toggles.

## 12. Reproducibility and Iceberg considerations

Conceptual reproducibility path for curated outputs:

- identify source input snapshot(s) (`intermediate.site_apportioned_responses` and any explicit refs/ops tables);
- capture run config values that affect aggregation semantics;
- execute and store asset check outcomes;
- materialise curated output snapshot in Iceberg;
- later attach release labels/tags to selected output snapshots.

Iceberg time-travel/snapshot capability is the core reason curated tables are canonical outputs. CSV/Excel exports (if needed later) should be generated from a specific curated table snapshot. Future APIs should query curated outputs (or controlled serving views over them), not become system-of-record stores.

No snapshot-management implementation is introduced in this PR.

## 13. API considerations (future-facing)

- API work is out of scope for v1 seam.
- Future APIs should expose curated statistical concepts, not raw pipeline internals.
- API reads should come from curated Iceberg-backed tables (or controlled serving views).
- Avoid adding a separate serving database until there is a clear performance/security requirement.
- Curated contract design should keep downstream API mapping straightforward.

## 14. Risks and open questions

- ambiguity in output grain across legacy publication outputs;
- parity risk when comparing against legacy CSV/Excel artefacts;
- hidden dependencies in legacy output config/path/schema plumbing;
- survey-type branch complexity (BERD vs PNP) for canonical curated contract scope;
- whether short-form/long-form/intramural families should become separate curated tables or remain export views from curated core;
- whether v1 should remain one proof-of-value table or include broader output families;
- how to demonstrate numerical equivalence with legacy references (synthetic + golden tests);
- future release tagging and snapshot-retention policies.

## 15. Acceptance criteria for this planning PR

- Documentation-only PR.
- New ADR exists at `docs/architecture/0017-curated-output-iceberg-seam-inventory.md`.
- ADR identifies relevant legacy output/finalisation modules/functions.
- ADR separates aggregation/business logic from orchestration/I-O/CSV/Excel plumbing.
- ADR recommends first clean curated Iceberg seam (`curated.rnd_statistics`) with justification.
- ADR proposes future fixture, asset, contract, and check strategy.
- ADR applies Dagster-native guidance from ADR 0014.
- ADR discusses reproducibility, Iceberg point-in-time traceability, and future API access.
- ADR explicitly states CSV/Excel exports are not canonical output targets.
- No runtime code/tests/assets/checks/contracts changed.


# ADR 0015: Outlier / estimation seam inventory and first clean implementation plan

- **Status:** Proposed
- **Date:** 2026-05-24

## 1. Purpose

This ADR documents a focused inventory of the legacy outlier detection and estimation area to prepare the next **clean seam extraction** after the implemented refoundation slice:

- `intermediate.staged_responses`
- `intermediate.mapped_responses`
- `intermediate.imputed_responses`

This ADR is planning and inventory only.

It is explicitly:

- **not** a migration of old stage runners;
- **not** a wrapper around legacy orchestration;
- **not** an implementation PR for outlier handling or estimation;
- **not** a runtime code change.

Legacy outlier/estimation code is treated as a **parity/reference oracle** during migration only. The target production input for this area is canonical `intermediate.imputed_responses`.

## 2. Current legacy role (conceptual)

### 2.1 Inputs consumed

In the legacy pipeline (`src/pipeline.py`), outlier/estimation runs after imputation for BERD survey runs:

- primary response frame (post-imputation);
- manual outlier file (optional, loaded in staging);
- config values for clipping thresholds, flag columns, output toggles, and QA output paths.

### 2.2 Outputs produced

Legacy outlier/estimation currently produces:

- transformed response dataframe with `outlier`, `a_weight`, `g_weight` columns for downstream use;
- optional QA CSV artefacts:
  - automatic outlier extract,
  - outlier QA extract,
  - estimation weights QA,
  - full estimation QA;
- weighted responses used by downstream site apportionment and later output formatting.

Outputs are intermediate transformed data + QA files (not publication-ready tables by themselves).

### 2.3 Coupling between outliers and estimation

Outlier and estimation are separate modules but analytically coupled:

- estimation requires outlier decisions (`outlier` boolean);
- outlier module creates intermediate auto flags and final resolved `outlier` state.

The coupling seam is clear: **estimation consumes final outlier-labelled responses**.

### 2.4 Manual inputs and overrides

Legacy behaviour includes manual outlier override integration:

- optional manual outlier file loaded in staging;
- merge by `reference`;
- final `outlier` decided by precedence:
  1) `manual_outlier`, then
  2) `auto_override_outlier_status`, then
  3) `auto_outlier`.

### 2.5 Config dependencies

Key legacy config dependencies include:

- `outliers.upper_clip`, `outliers.lower_clip`;
- developer `outliers.flag_cols` (outlier value columns);
- output toggles:
  - `global.output_auto_outliers`
  - `global.output_outlier_qa`
  - `global.output_estimation_qa`
  - `global.load_manual_outliers`
- path groups:
  - `outliers_paths.*`
  - `estimation_paths.*`
- estimation numeric-column selection through breakdown config (`get_all_wanted_columns(config, "estimation")` and `...("employment")`).

### 2.6 Survey-type branching and PNP/BERD/NI

In legacy orchestration:

- BERD: outlier + estimation executed;
- PNP: outlier + estimation skipped;
- NI is loaded/processed earlier and not a dedicated outlier/estimation branch in these modules.

This indicates survey-type branching exists in orchestration and is a future design decision for lean assets/checks.

## 3. Function-level inventory

### 3.1 Orchestration entrypoints and stage runners

| File | Function | Current role | Inputs | Outputs | Side effects | Validation | Config deps | I/O deps | Type | Proposed target |
|---|---|---|---|---|---|---|---|---|---|---|
| `src/pipeline.py` | `run_pipeline` (outlier/estimation section) | Sequentially invokes outlier then estimation for BERD; skips for PNP | imputed df, manual outliers df, config, write fns | estimated df passed downstream | logging, output file writes via called modules | indirect only | survey type + global toggles | file/platform abstraction through `mods` | orchestration mixed with runtime wiring | **Keep legacy only** (do not port stage runner pattern) |
| `src/outlier_detection/outlier_main.py` | `run_outliers` | Orchestrates auto outliers, optional QA export, manual override merge, final outlier column prep | response df, manual outliers df, config, `write_csv` | df with final `outlier` and temp outlier flags dropped | writes auto/QA csv files | uses auto config validation indirectly | outliers thresholds, flag cols, output toggles, paths | explicit csv writing | mixed (domain + orchestration + file I/O) | split: domain functions -> `domain/outliers/`; exports/checks -> assets/checks; file-path QA outputs deprecated |
| `src/estimation/estimation_main.py` | `run_estimation` | Orchestrates weight calc + optional QA exports | outlier-labelled df, config, `write_csv` | weighted df (plus QA df internally) | optional qa csv writes | indirect in weight functions | output toggle + estimation paths + breakdown config | explicit csv writing | mixed | split: calc/apply pure logic -> `domain/estimation/`; QA visibility -> checks/metadata/tables |

### 3.2 Outlier logic functions

| File | Function | Role classification | Inputs -> outputs | Side effects / dependencies | Proposed target |
|---|---|---|---|---|---|
| `src/outlier_detection/auto_outliers.py` | `validate_config` | config validation helper | clips + flag cols -> exceptions/logging | raises `ImportError`; logs | rewrite as explicit domain validation + config schema/check |
| `src/outlier_detection/auto_outliers.py` | `filter_valid` | statistical filtering rule | df + value col -> valid subset | raises `ValueError` if empty | `domain/outliers/` |
| `src/outlier_detection/auto_outliers.py` | `get_clip_bands` | clip-band arithmetic | grouped counts + clip -> rows/band cols | mutates frame copy semantics | `domain/outliers/` |
| `src/outlier_detection/auto_outliers.py` | `flag_outliers` | core auto-flagging per variable | df + clips + value col -> per-col flag merged back | grouping, ranking, merge; depends on column names | `domain/outliers/` |
| `src/outlier_detection/auto_outliers.py` | `decide_outliers` | combine per-col flags | df + flag cols -> `auto_outlier` | none | `domain/outliers/` |
| `src/outlier_detection/auto_outliers.py` | `log_outlier_info` | diagnostic logging | df + value col -> none | logs + reuses filter_valid | replace with asset metadata/check diagnostics |
| `src/outlier_detection/auto_outliers.py` | `run_auto_flagging` | orchestration over per-col flagging | df + config values -> df with flags | logging; loops over config cols | split wrapper removed; compose domain functions in lean asset |
| `src/outlier_detection/auto_outliers.py` | `apply_short_form_filters` | QA extract filter utility | df -> filtered df | none | likely keep as helper in assets/checks or deprecated if QA table design differs |
| `src/outlier_detection/auto_outliers.py` | `normal_round` | arithmetic helper | float -> int | none | `domain/outliers/` helper |
| `src/outlier_detection/manual_outliers.py` | `apply_manual_outliers` | core precedence merge for final outlier | merged df with manual/auto cols -> final `outlier` | logs counts | `domain/outliers/` (high-value first seam candidate) |

### 3.3 Estimation logic functions

| File | Function | Role classification | Inputs -> outputs | Side effects / dependencies | Proposed target |
|---|---|---|---|---|---|
| `src/estimation/calculate_weights.py` | `create_weights_filter` | business rule filter | df -> mask | depends on `selectiontype`, `formtype` | `domain/estimation/` |
| `src/estimation/calculate_weights.py` | `create_estimation_filter` | business rule filter | df -> mask | depends on status/instance/709 validity | `domain/estimation/` |
| `src/estimation/calculate_weights.py` | `calc_lower_n/e/s` | formula components | filtered group -> scalar | none | `domain/estimation/` |
| `src/estimation/calculate_weights.py` | `calc_a_weight` | core a-weight formula per cell | cell group -> cell group with `a_weight` | grouped mutation semantics | rewrite as explicit domain transformation |
| `src/estimation/calculate_weights.py` | `calc_g_weight` | core g-weight formula per cell | cell group -> with `g_weight` | grouped mutation semantics | rewrite as explicit domain transformation |
| `src/estimation/calculate_weights.py` | `calculate_weighting_factors` | end-to-end weight orchestration + QA frame | df -> (weighted df, qa frame) | requires `outlier`; sets defaults; drops temp cols | split into pure weighting output + explicit QA table function |
| `src/estimation/calculate_weights.py` | `create_weights_qa_df` | QA table formatter | df -> QA frame | renaming/reporting semantics | future `qa.*` asset or check metadata |
| `src/estimation/calculate_weights.py` | `outlier_weights` | rule to set weights for outliers | df -> df | depends on outlier bool | `domain/estimation/` |
| `src/estimation/apply_weights.py` | `apply_weights` | applies a/g weights to estimation/employment columns and recalculates totals | df + config -> weighted df | depends on breakdown config helper | split: pure apply helper in domain; config parsing in asset layer |

### 3.4 Helper/module dependencies relevant to this area

| File | Function/module | Why relevant | Proposed target |
|---|---|---|---|
| `src/staging/staging_main.py` | manual outlier file load + schema validation | source of manual outlier input in legacy flow | future explicit `ops.manual_outliers` input asset/table |
| `src/utils/breakdown_validation.py` | `get_all_wanted_columns`, `calc_totals` | estimation numeric-column selection and total recomputation | either extracted shared domain utility or rewritten targeted estimation helpers |
| `config/test_configs/*.yaml` | outlier/estimation params and toggles | exposes legacy config surface and file-era output controls | replace path/toggle plumbing with Dagster config + checks + table contracts |

## 4. Business-logic vs legacy-plumbing classification

### 4.1 Keep/extract statistical logic

- outlier valid-record filters, clipping maths, ranking/threshold flagging, final outlier decision logic;
- estimation formulas (`a_weight`, `g_weight`, outlier handling in weighting);
- weighted application over selected numeric columns.

### 4.2 Rewrite as clean domain functions

- reshape groupby/apply mutation into explicit, testable pandas transforms;
- isolate column-name assumptions in typed contracts or constants;
- make precedence and rule logic explicit in function signatures.

### 4.3 Replace with Dagster assets/checks

- stage runner wrappers (`run_outliers`, `run_estimation`) become thin lean assets over domain functions;
- logging-only diagnostics become asset checks + materialisation metadata;
- QA CSV outputs become Iceberg QA tables and/or check outputs.

### 4.4 Replace with explicit Iceberg input tables

- manual outlier file -> `ops.manual_outliers` (or `ops.outlier_overrides`);
- optional parameter/reference tables as needed, e.g. `ref.estimation_parameters` when configuration must be data-driven.

### 4.5 Delete/deprecate legacy file/config plumbing

- `*_paths` folder/file routing for outlier/estimation QA;
- global output toggles controlling csv side effects;
- platform-specific write-csv dependencies for this stage area.

## 5. Recommended first clean seam

## Recommendation: **minimal manual-outlier-adjustment seam first**

Start with a focused seam that consumes canonical `intermediate.imputed_responses` and optional `ops.manual_outliers`, producing `intermediate.outlier_adjusted_responses`.

Why this is the smallest useful seam:

1. It proves an analytically meaningful behaviour immediately (manual override precedence on outlier status).
2. It avoids coupling to full automatic clipping logic and full estimation formulas in the first step.
3. It is pandas-only and straightforward to test with tiny synthetic fixtures.
4. It sets a stable input contract for later automatic outlier and estimation steps.
5. It aligns with production operator needs (explicit operational override inputs with checks).

Scope for next implementation PR (not this PR):

- domain function only for manual outlier merge/precedence;
- thin asset wrapper;
- minimal checks;
- no automatic clip flagging yet;
- no estimation yet.

If hidden dependencies appear during implementation, allow a narrower spike limited to contract confirmation (column/grain/provenance), but current inventory suggests manual seam is feasible directly.

## 6. Proposed future table contracts (documented only)

### 6.1 Recommended next table: `intermediate.outlier_adjusted_responses`

**Proposed grain:**

- `reference + instance + survey_type + survey_year` (same canonical grain carried from imputed stage).

**Likely required columns (minimum concept):**

- existing canonical response columns from `intermediate.imputed_responses`;
- `outlier` (boolean, final decision);
- `outlier_source` (enum-like text: `auto`, `manual_outlier`, `manual_override`, `default_none`);
- `outlier_reason` (nullable text for operator context);
- `outlier_adjustment_applied` (boolean);
- optional provenance fields if available:
  - `manual_outlier_id` / `override_id`,
  - `manual_effective_period`.

### 6.2 Conceptual future ops/ref inputs

- `ops.manual_outliers`
  - key columns: response grain keys;
  - adjustment column(s): explicit outlier decision;
  - provenance: reason, requester/approver, timestamp/effective period.
- optional `ops.outlier_overrides`
  - if team wants separation between positive flags and overrides.
- optional `ref.estimation_parameters`
  - only if estimation formula controls become table-driven rather than Dagster config.

No table-contract YAML changes are included in this PR.

## 7. Synthetic fixture requirements (future PR)

Proposed future scenario path:

- `tests/fixtures/synthetic/scenarios/imputation_to_outlier_minimal/`

Proposed files:

- `input_imputed_responses.csv`
- `input_ops_manual_outliers.csv`
- `expected_outlier_adjusted_responses.csv`
- `scenario.yaml` (brief intent/coverage metadata)

Minimal cases to include:

1. record with manual `True` overriding auto/default `False`;
2. record with manual `False` overriding auto/default `True`;
3. record with no manual row retaining default behaviour;
4. duplicate/manual invalid key case for check coverage;
5. instance-specific behaviour (at least one `instance != 0` row).

Why minimal and safe:

- tiny synthetic records only;
- no real identifiers/production paths;
- deterministic and human-readable expected output.

## 8. Target asset/check rollout sequence (future, not implemented)

1. Add pure pandas domain function in `src/randd_pipeline/domain/outliers/`.
2. Add thin Dagster asset consuming `intermediate.imputed_responses` (+ ops table) and materialising `intermediate.outlier_adjusted_responses`.
3. Add/update table contract definitions for new table.
4. Add first operator-facing checks.
5. Add synthetic chain smoke test from imputation -> outlier-adjusted stage.
6. Later PR: add automatic outlier flagging seam or estimation seam using this output.

## 9. Operator-facing check ideas (future)

Suggested initial checks for outlier-adjusted stage (operator-friendly naming):

- `outlier_adjusted_required_columns_present`
- `outlier_adjusted_unique_response_grain`
- `outlier_flag_populated_for_all_records`
- `manual_outlier_references_exist_in_imputed_input`
- `manual_outlier_rows_are_unique_on_grain`
- `manual_adjustment_reason_present_when_override_used`
- `no_invalid_negative_values_after_outlier_adjustment` (if value adjustment is introduced later)

Future estimation-stage checks (when estimation is introduced):

- `estimated_required_columns_present`
- `estimated_unique_output_grain`
- `a_and_g_weights_populated_for_target_rows`
- `estimated_values_non_negative_where_required`
- `estimation_class_coverage_complete`
- `estimated_totals_reconcile_within_tolerance`

## 10. Dagster-native design considerations (ADR 0014 aligned)

### 10.1 What should be Dagster Config

Use Dagster `Config` for operator-facing controls only, e.g.:

- enable/disable manual outlier application for a run mode;
- optional strictness level for missing manual references;
- future estimation run controls where operator override is legitimate.

Avoid reintroducing loose file-path toggles for QA output.

### 10.2 What should be a Resource

- table read/write boundaries should use `TableStoreResource`;
- future external connectors (if needed) stay as resources, not embedded in assets.

### 10.3 What should be asset checks

- schema/grain/reference integrity;
- override provenance quality;
- weight/output validity once estimation exists.

### 10.4 Materialisation metadata to attach

For this stage area, useful metadata includes:

- input/output row counts;
- number of manual override rows supplied;
- number of overrides applied;
- outlier rate summary;
- survey year/type and run purpose;
- source table identifiers/snapshot IDs (when available).

### 10.5 Likely future blocking checks

Likely blocking later:

- missing required columns;
- duplicate response grain;
- manual override references not present in input;
- invalid outlier value types;
- impossible weighted outputs (when estimation stage lands).

### 10.6 Partitioning considerations

Partitioning by survey year and potentially survey type is likely relevant for this stage, especially because legacy behaviour branches by survey type.

## 11. Risks and open questions

### 11.1 Risks

- statistical correctness risk when extracting formulas from grouped mutation logic;
- parity/equivalence risk if legacy side conditions are implicit;
- hidden config/file dependency risk (legacy toggles and output paths);
- manual-input provenance risk if operational corrections are underspecified;
- branch divergence risk for BERD/PNP/NI expectations.

### 11.2 Open questions

1. Should automatic outliering and estimation remain separate production-facing stages, or is a combined stage ever justified?
2. What minimum ops provenance fields are mandatory for manual outlier records?
3. Should PNP continue bypassing outlier/estimation, or should lean pipeline support explicit no-op assets/check behaviour by survey type?
4. Which legacy QA outputs must be represented as checks vs `qa.*` tables?
5. What tolerance/metric definitions should govern parity for future estimation outputs?

### 11.3 Equivalence evidence approach (future PRs)

Use both:

- tiny synthetic scenario assertions (exact expected outputs);
- legacy-oracle reconciliation on selected golden extracts where permissible;
- explicit statement of expected numerical equivalence per migrated seam.

## 12. Acceptance criteria

- [x] Documentation-only PR.
- [x] New ADR at `docs/architecture/0015-outlier-estimation-seam-inventory.md`.
- [x] Inventory identifies relevant legacy outlier/estimation modules/functions.
- [x] Inventory separates business/statistical logic from orchestration/I/O/config plumbing.
- [x] ADR recommends first clean seam (minimal manual-outlier-adjustment seam).
- [x] ADR proposes future fixture, asset, contract, and check strategy.
- [x] ADR applies Dagster-native guidance from ADR 0014.
- [x] No runtime code changed.
- [x] No tests changed.
- [x] No assets/checks/contracts changed.
- [x] Intended PR mode: draft against `develop`.

## Implementation update (manual seam v1)
- First manual-outlier adjustment seam is implemented for `imputed_responses -> outlier_adjusted_responses`.
- Scope: full-grain joins (`reference+instance+survey_type+survey_year`) with explicit `ops.manual_outliers` overrides and provenance columns (`outlier_source`, `outlier_adjustment_applied`, `outlier_reason`).
- Non-scope: automatic outlier detection/clipping, estimation, weighting, and downstream QA outputs.
- Follow-up areas: governed operational workflow for manual inputs, richer QA checks/metrics, and integration with later estimation seams.

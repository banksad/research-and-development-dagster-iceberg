# ADR 0013: Imputation seam inventory for lean refoundation

- **Status:** Proposed
- **Date:** 2026-05-23

## 1. Purpose

This ADR provides a focused inventory and implementation plan for the first clean imputation seam after mapping in the lean Dagster/Iceberg refoundation.

Scope intent:

- This is **documentation-only** and does **not** implement imputation.
- This is **not** a migration of the legacy `run_imputation` runner as-is.
- This is **not** a wrapper around legacy orchestration and file-era plumbing.
- Legacy imputation modules are treated as a **parity/reference oracle** only.
- The target production-facing upstream input is canonical `intermediate.mapped_responses`.

## 2. Current legacy role of imputation

Conceptually, legacy imputation sits between mapping and outliering/estimation and performs multiple responsibilities in one stage:

1. Pre-imputation derived-value preparation (including apportionment and short-to-long handling).
2. Manual trim application for selected responses.
3. Backdata-based imputation (MoR + CF behaviour where applicable).
4. Trimmed Mean Imputation (TMI) where MoR/CF does not apply.
5. BERD-only short-form expansion imputation.
6. Imputation QA outputs, backdata creation, and breakdown validation.

### Consumed inputs (legacy)

- Mapped responses dataframe (`mapped_df` from mapping).
- Manual trimming dataframe (`manual_trimming_df`, optional).
- Backdata dataframe (`backdata`, optional but used for MoR/CF).
- Config sections spanning global/survey/imputation/path/schema switches.

### Produced outputs (legacy)

- Returned imputed responses dataframe (later consumed by construction/outliers/estimation depending on survey type).
- Optional CSV QA side outputs (full imputed responses, trim QA, wrong-604 QA, links QA, trim counts).
- Optional generated backdata CSV for future periods.

### Key variable/column patterns

- Target value columns are config-driven (`lf_target_vars`, `sum_cols`, derived imputation col list).
- Uses `reference` + `instance` grain heavily; also `formtype`, `status`, `imp_class`, `imp_marker`.
- Uses prior-period suffix columns (`*_prev`) during carry-forward/link flows.
- Builds and updates marker/status-style columns such as `imp_marker`, `manual_trim`, and `200_imp_marker`.

### Backdata / prior-period dependence

- MoR/CF branch depends on backdata availability and minimum class threshold.
- Legacy logic includes survey/year-specific behaviour (e.g., LF-only handling for PNP and BERD 2022 condition).

### BERD vs PNP differences

- BERD runs short-form expansion imputation.
- PNP path has R&D-type-specific handling and different practical form mix assumptions.

### Config and file side effects

- Depends on multiple config keys across `global`, `survey`, `imputation`, `imputation_paths`, and `schema_paths`.
- Produces file outputs via injected CSV writer; this is legacy file-era operational behaviour to phase out.

## 3. Function-level inventory (legacy to target)

The inventory below focuses on modules/functions that define the clean-seam boundary and migration priorities.

| File path | Function(s) | Current role | Inputs | Outputs | Side effects / validation / deps | Type | Proposed target |
|---|---|---|---|---|---|---|---|
| `src/imputation/imputation_main.py` | `run_imputation` | Stage orchestrator for apportionment, short-to-long, prep, manual trim, MoR, TMI, SF expansion, QA, backdata, tidy/validation | mapped df, manual trim df, backdata, config, `write_csv` | imputed df | Writes multiple QA/backdata CSVs; invokes breakdown validation; reads schema config | Mixed (orchestration + pandas + file I/O) | **Split**: orchestration into future `src/randd_pipeline/assets/`; pure transforms extracted to domain; QA/check logic to `src/randd_pipeline/checks/`; file outputs deprecated |
| `src/imputation/imputation_helpers.py` | `get_imputation_cols`, `create_imp_class_col`, `imputation_prep`, `fix_604_error`, `calculate_totals`, `tidy_imputation_dataframe`, `imputation_marker`, `create_new_backdata`, etc. | Shared prep/cleanup/utility logic across imputation flows | df + config | transformed df(s), masks, helper cols | Some pure logic; some coupled to legacy column conventions and backdata creation | Mixed (mostly pandas/domain + some legacy plumbing) | **Extract/rewrite selectively** into `src/randd_pipeline/domain/imputation/`; backdata writer-oriented parts become explicit table-building logic |
| `src/imputation/MoR.py` | `run_mor`, `mor_preprocessing`, `carry_forwards`, `calculate_growth_rates`, `calculate_links`, `apply_links`, `calculate_mor` | Backdata-driven MoR/CF imputation pipeline | current df, backdata df, config | imputed df + links QA df | threshold validation from config; joins current/prior; class-size logic | Mixed (core statistical logic + orchestration) | **Core keep/extract** to domain functions; links QA surfaced as check metadata/QA table; backdata input replaced by explicit `raw.backdata` contract |
| `src/imputation/tmi_imputation.py` | `run_tmi`, `tmi_prep`, `run_longform_tmi`, `run_shortform_tmi`, `trim_bounds`, `create_mean_dict`, `apply_tmi` | TMI preparation, trimming, class-level mean creation and application | df, config | imputed df + QA outputs | uses manual trim state and class grouping; produces QA diagnostics | Mostly pandas/statistical + some orchestration coupling | **Primary first seam candidate** for domain extraction (narrowed subset first), then expand coverage |
| `src/imputation/sf_expansion.py` + `src/imputation/expansion_imputation.py` | `run_sf_expansion`, `run_expansion`, `evaluate_imputed_ixx` | BERD short-form expansion of 2xx/3xx breakdowns post TMI | imputed df, config | df with expanded breakdown imputations | depends on TMI markers and target columns | Mostly pandas/statistical | Keep for later PR (after first seam) in `domain/imputation/` |
| `src/imputation/short_to_long.py` | `run_short_to_long` | Converts/duplicates SF records to LF-compatible structure | df | expanded/reshaped df | structural row/instance impact | pandas transform | Keep legacy for now; evaluate as separate seam later |
| `src/imputation/apportionment.py` | `run_apportionment` (+ helpers) | Derives FTE/headcount columns from 4xx/5xx values pre-imputation | df | df with derived columns | deterministic arithmetic rules | pandas/domain | Candidate for dedicated pre-imputation domain module; not in first seam |
| `src/imputation/manual_imputation.py` | `merge_manual_imputation`, `join_manual_trim_df_for_qa` | Applies manual trim flags and merges trimmed rows into QA outputs | df, manual trim df, QA dfs, config | updated df/QA dfs | depends on optional manual trim table | mixed | manual trim input should become explicit `ops.manual_trimming`-style table and separate clean apply function |
| `src/imputation/impute_civ_def.py` | `impute_civil_defence` (+ helpers) | R&D type (200) imputation for specific conditions using class proportions/random assignment | df | df with imputed `200` and markers | stochastic assignment by reference-based seed pattern and class dictionaries | statistical/domain | Keep legacy-only initially unless required for first production seam; evaluate determinism requirements |
| `src/pipeline.py` | `run_pipeline` call-site to `run_imputation` | Legacy orchestration sequence and stage wiring | full pipeline state/config | passes mapped->imputed stage output forward | legacy orchestration root | orchestration | Keep legacy only as oracle; **do not** extend for refoundation |
| `src/staging/staging_main.py` + `src/utils/path_helpers.py` | backdata/manual trim loading and path selection | Legacy source loading for imputation dependencies | config/path switches + file readers | in-memory tables for downstream | file existence checks, schema validation, survey-specific path branching | legacy I/O/orchestration | Replace with explicit Iceberg `raw.*` / `ops.*` assets |

## 4. Business-logic vs legacy-plumbing classification

### A) Keep/extract statistical logic

- Class-based MoR/CF link computation (`calculate_growth_rates`, `calculate_links`, `apply_links`).
- TMI class trimming + mean application (`trim_bounds`, mean dict/application flow).
- Deterministic derived-value recomputation used after imputation (`calculate_totals` equivalent behaviour).

### B) Rewrite as clean domain function(s)

- Imputation preparation currently entangled in `imputation_prep` should be split into explicit, typed dataframe transforms.
- Marker population (`imp_marker` transitions, imputed/not-imputed flags) should be explicit and stage-contract driven.
- Manual trim application should become a minimal table-driven merge/apply function.

### C) Replace with Dagster asset/check behaviour

- Breakdown consistency validation currently run inline should become asset checks.
- Legacy QA-dataframe side outputs should be represented as check metadata and/or dedicated `qa.*` tables.
- Threshold/config validation failures should be converted to operator-readable check failures where appropriate.

### D) Replace with explicit Iceberg input table(s)

- Backdata dependency -> canonical `raw.backdata` input table.
- Manual trimming dependency -> explicit `ops.manual_trimming` (or equivalent) input table.
- Any static/imputation reference parameters -> explicit `ref.*` table if needed.

### E) Delete/deprecate legacy file/config plumbing

- `imputation_paths.*` file-output dependencies for QA/backdata CSVs.
- File-path-driven conditional loading patterns for manual trim/backdata.
- Stage-internal output file writes from imputation runtime logic.

## 5. Recommended first clean imputation seam (next PR)

### Recommendation

Adopt a **minimal TMI-first seam** implemented as pure pandas domain logic that:

1. Consumes canonical `intermediate.mapped_responses`.
2. Applies one narrow, deterministic TMI behaviour on a tiny subset of columns/grain.
3. Produces `intermediate.imputed_responses` with explicit imputation status marker(s).

### Why this seam

- Avoids immediate entanglement with full MoR/backdata complexity.
- Provides meaningful imputation behaviour (not just passthrough).
- Keeps scope aligned with a small, testable domain function and single stage output.
- Supports rapid operator-facing checks for imputation completeness/validity.

### Narrow first behaviour to prove

- One target variable family (e.g., a single total variable) for non-responders with a clear `imp_marker='TMI'` rule.
- Explicit “no imputation applied” marker for unaffected records.
- No BERD-only expansion, no civ/def imputation, no outlier/estimation/apportionment/output coupling.

### If confidence is insufficient

If function disentanglement reveals hidden coupling too large for safe immediate extraction, run a smaller spike PR that only defines and validates:

- exact minimal columns required from `intermediate.mapped_responses`;
- deterministic marker semantics;
- one classing strategy contract;
- equivalence harness design against legacy on synthetic test cases.

## 6. Proposed future table contracts (documented only)

> This ADR proposes future contract shape only. No contract file changes in this PR.

### `intermediate.imputed_responses` (proposed)

**Grain:** one row per `reference`, `instance`, `period` (or current mapped-response grain equivalent).

**Likely required columns:**

- Key/grain: `reference`, `instance`, survey-period identifiers.
- Core pass-through classification: `formtype`, `status`, key mapping outputs needed downstream.
- Imputation outputs: selected `*_imputed` numeric columns for configured target variables.
- Method markers: `imp_marker` (e.g., `no_imputation`, `CF`, `MoR`, `TMI`), optional per-variable markers where needed.
- QA-friendly indicators: booleans/counts such as `is_imputed`, `imputation_method`.

### `raw.backdata` (proposed refinement)

**Grain:** one row per prior-period response key (`reference`, `instance`, prior period).

**Likely required columns:**

- join keys: `reference` (and `instance` if required by business logic), prior period metadata;
- prior values for target variables;
- prior `imp_marker`/form metadata needed for MoR/CF filtering.

### `ops.response_corrections` / `ops.manual_trimming` (imputation-relevant input)

**Grain:** correction event row keyed to response grain + column scope.

**Likely required columns:**

- response keys (`reference`, `instance`, period);
- correction type (`manual_trim`, value override, etc.);
- target column/value metadata;
- reason, requester/approver, timestamp, status.

### Optional imputation reference table (if needed)

- `ref.imputation_parameters` could externalise class exclusions, thresholds, and method toggles currently hidden in config.

## 7. Synthetic fixture requirements (future PR)

Proposed minimal scenario path:

`tests/fixtures/synthetic/scenarios/mapping_to_imputation_minimal/`

Proposed files:

- `input_mapped_responses.csv`
- `input_backdata.csv` (only if seam uses MoR/CF; omit for TMI-first seam)
- `input_manual_trimming.csv` (only if manual trim behaviour in scope)
- `expected_imputed_responses.csv`

Minimal cases to include:

1. clear responder (no imputation)
2. non-responder imputed via selected seam method
3. record excluded from imputation class (remains not imputed with marker)
4. deterministic tie/rounding scenario for stable expected output

Why minimal/non-sensitive:

- tiny synthetic rows only;
- no production identifiers or paths;
- just enough coverage to validate method + marker semantics and regressions.

## 8. Target asset/check rollout sequence (future implementation)

1. Add first domain function in `src/randd_pipeline/domain/imputation/` for selected seam.
2. Add `intermediate.imputed_responses` asset in `src/randd_pipeline/assets/` wired from `intermediate.mapped_responses`.
3. Document and then implement table contract update for imputed output (separate PR step).
4. Add first imputation asset checks in `src/randd_pipeline/checks/`.
5. Extend synthetic chain smoke test from mapping->imputation.
6. Add parity reconciliation evidence vs legacy oracle output for seam-covered behaviour.

## 9. Operator-facing imputation check ideas (plain language)

Suggested operator-readable check names:

- `imputed_required_columns`
- `imputed_unique_response_grain`
- `imputation_marker_populated`
- `no_illegal_missing_post_imputation`
- `imputed_values_non_negative`
- `imputation_rate_reported`

Suggested behaviour:

- blocking failures for missing required columns / non-unique grain / illegal nulls in required imputed fields;
- warning-level signal for unusual imputation rates or large method shifts versus baseline;
- metadata outputs containing counts and percentages by method (`no_imputation`, `TMI`, `MoR`, `CF`).

## 10. Risks and open questions

### Statistical correctness risk

- Extracting partial TMI/MoR behaviour can silently alter classing, trim bounds, or marker semantics.

### Parity/equivalence risk

- Legacy imputation bundles many sub-steps; proving equivalence requires seam-scoped synthetic/golden comparisons plus targeted legacy reconciliation.

### Hidden dependency risk

- Some behaviour currently depends on implicit config/path defaults and optional files (manual trim/backdata) loaded upstream.

### Backdata availability and contract risk

- MoR/CF rollout depends on consistent prior-period table availability and stable join keys.

### Survey-type divergence risk

- BERD and PNP treatment differs in legacy logic; first seam must explicitly bound which behaviour is in/out-of-scope.

### Warning vs blocking semantics

- Need policy for when imputation anomalies should fail stage versus surface operator warning only.

### Equivalence evidence approach

- Use tiny synthetic fixtures for deterministic unit coverage and optional golden-test comparison against legacy outputs for seam-specific variables/markers.

## 11. Acceptance criteria for this PR

- Documentation-only PR.
- New ADR added at `docs/architecture/0013-imputation-seam-inventory.md`.
- Relevant legacy imputation modules/functions inventoried.
- Statistical/business logic clearly separated from orchestration/I/O/config plumbing.
- First clean seam recommendation documented (or narrower spike trigger documented).
- Future fixture, asset, contract, and check strategy proposed.
- No runtime code changes.
- No tests changed.
- No assets/checks/contracts changed.
- Draft PR should target `develop`.

## 12. Implementation status update (minimal seam landed)

A first minimal clean imputation seam has now landed in the lean refoundation runtime:

- Domain function: `apply_simple_tmi_imputation(...)` in `src/randd_pipeline/domain/imputation/simple_tmi.py`.
- Asset: `intermediate/imputed_responses` materialised from `intermediate.mapped_responses`.
- Behaviour: deterministic simple class-mean imputation for a single synthetic numeric target (`601`) with marker output (`imp_marker`).
- Markers currently used: `not_imputed`, `TMI`, `no_mean_found`.
- Contract/check coverage now includes required columns, non-empty, unique grain, marker population, and illegal missing-imputed-value detection.

What this implementation does **not** include:

- Full legacy `run_imputation` migration.
- MoR/carry-forward, backdata output generation, manual trimming integration.
- Short-form expansion, apportionment, outlier/estimation/output-stage behaviour.

Follow-up areas:

1. Expand target-variable coverage beyond the single synthetic seam column.
2. Design explicit production config/parameter surface for method controls.
3. Introduce parity reconciliation harness outputs against legacy oracle behaviour per enabled method.
4. Add later seams for MoR/carry-forward and manual-trim flows via explicit input tables.

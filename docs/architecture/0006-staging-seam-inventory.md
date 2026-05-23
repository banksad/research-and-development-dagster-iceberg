# 0006: Staging seam inventory and extraction plan

- **Status:** Proposed
- **Date:** 2026-05-23
- **Type:** Planning / inventory only (no runtime migration)
- **Depends on:** 0002 migration principles, 0003 refoundation strategy, 0004 legacy deprecation map, 0005 business-logic inventory

## 1) Purpose

This note defines the preparation step before extracting staging business logic into the lean refoundation package (`src/randd_pipeline/`). It creates a function-level inventory of legacy staging code, classifies each element by responsibility, and proposes the first small extraction seam.

This is explicitly **documentation-only**:
- no staging logic is migrated yet,
- no legacy code is moved/copied yet,
- no new staging Dagster assets/checks are added yet,
- `src/pipeline.py` and wrapper definitions remain unchanged.

## 2) Current legacy staging role

Conceptually, the current staging stage performs the following sequence:

1. Loads raw response snapshot data (JSON or feather shortcut path under some run modes/platforms).
2. Validates schema and shape of loaded response structures.
3. Parses contributors/responses and transposes long response rows into wide response columns.
4. Harmonises and validates postcode fields against a postcode mapper/masterlist.
5. Loads manual inputs and reference mappers (for outliers, trimming, backdata, mapper tables).
6. Prepares staged response dataframes and auxiliary inputs consumed by mapping/imputation downstream.
7. Emits optional QA/output CSV artefacts where configured.

This stage is currently entangled with legacy run-mode flags, platform-specific file adapters, and optional frozen/updated snapshot pathways.

## 3) Function-level inventory

### A. Orchestration entrypoint and stage wiring

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|
| `src/staging/staging_main.py` | `run_staging(...)` | Legacy stage wrapper orchestrating load/validate/postcode/manual-files/QA flow | `config`, `rd_*` file functions | tuple: `full_responses`, `manual_outliers`, `postcode_mapper`, `backdata`, `pg_detailed_mapper`, `sic_division_detailed_mapper`, `manual_trim_df` | logging, reads/writes csv/feather/json, conditional early branches by run mode | heavy use of `global`, `dev_global`, `staging_paths`, `mapping_paths`, `survey` keys | `rd_file_exists`, `rd_load_json`, `rd_read_csv`, `rd_write_csv`, `rd_read_feather`, `rd_write_feather` | orchestration wrapper | **keep legacy only** for parity oracle; later replace with thin `assets/staging.py` plus domain helpers |

### B. Snapshot parse/transmutation seams (candidate domain extraction)

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|
| `src/staging/spp_parser.py` | `parse_snap_data(snapdata)` | parse snapshot dict into contributors + responses dataframes | snapshot dict (`contributors`, `responses`) | `(contributors_df, responses_df)` | logging via decorators/logger | none | none | pure business transform | `src/randd_pipeline/domain/staging/snapshot_parse.py` |
| `src/staging/spp_snapshot_processing.py` | `create_response_dataframe(df, unique_id_cols)` | pivot long responses to wide question columns | merged df, id columns | response-wide df | none | none | none | pure business transform | `src/randd_pipeline/domain/staging/transforms.py` |
| `src/staging/spp_snapshot_processing.py` | `create_contextual_dataframe(df, unique_id_cols)` | retain non-response contextual cols and deduplicate | merged df, id columns | contextual df | none | none | none | pure business transform | `src/randd_pipeline/domain/staging/transforms.py` |
| `src/staging/spp_snapshot_processing.py` | `full_responses(contributors, responses)` | contributor/response merge + wide transmutation | contributors df, responses df | staged `full_responses` df | logging | none | none | pure business transform | `src/randd_pipeline/domain/staging/transforms.py` |
| `src/staging/spp_snapshot_processing.py` | `response_rate(contributors, responses)` | computes response ratio | contributors df, responses df | float response rate | logging | none | none | pure business transform | `src/randd_pipeline/domain/staging/metrics.py` |
| `src/staging/staging_helpers.py` | `load_val_snapshot_json(...)` | loads JSON + invokes parse/transmute + schema validation | `snapshot_path`, `load_json`, `config` | `(full_responses_df, response_rate_str)` | reads JSON, schema validation, logging | `dev_global.platform`, `dev_global.dev_test` | JSON loader callable | orchestration wrapper + validation composition | split: helper wrapper in `assets/staging.py`; transform internals in `domain/staging/*` |
| `src/staging/staging_helpers.py` | `load_snapshot_feather(feather_file, read_feather)` | feather shortcut loader | feather path + reader fn | snapshot dataframe | read + logging | none | feather read callable | file/platform I/O | keep legacy only; later thin table-based equivalent |
| `src/staging/staging_helpers.py` | `df_to_feather(...)` | writes feather cache | directory/save name/dataframe/writer | none | filesystem write | none | local FS + writer callable | file/platform I/O | keep legacy only; delete/deprecate later |

### C. Validation seams

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|
| `src/staging/validation.py` | `load_schema(file_path)` | load TOML schema dict | schema path | dict/None | file read + logs | none | local file path read | file/platform I/O | keep as legacy utility initially; consider shared schema loader under `checks/` |
| `src/staging/validation.py` | `check_data_shape(data_df, ...)` | compare dataframe columns vs contributor+wide schema set | dataframe + schema paths | bool (or error) | logging | none beyond passed schema paths | depends on `load_schema` file reads | validation logic | `src/randd_pipeline/checks/staging_checks.py` (adapted to table contracts later) |
| `src/staging/validation.py` | `validate_data_with_schema(survey_df, schema_path)` | dtype casting/validation against TOML schema | dataframe, schema path | mutated dataframe (in-place effects) | logging + coercive casts | none | schema file reads | validation logic (mutating) | `src/randd_pipeline/checks/schema_casting.py` (or replace by contract checks; human decision) |
| `src/staging/validation.py` | `combine_schemas_validate_full_df(...)` | combined contributors+wide dtype casting for full df | dataframe + two schema paths | mutated dataframe | logging + casts | none | schema file reads | validation logic (mutating) | `src/randd_pipeline/checks/schema_casting.py` (or replace; decision required) |
| `src/staging/validation.py` | `validate_many_to_one(*args)` | checks one-to-many mapper consistency | mapper df + column names | validation result dataframe/errors | logging/errors | none | none | validation logic | `src/randd_pipeline/checks/mapping_checks.py` |
| `src/staging/validation.py` | `validate_cora_df(df)` | validates CORA mapper uniqueness/keys | dataframe | dataframe/errors | logging/errors | none | none | validation logic | `src/randd_pipeline/checks/mapping_checks.py` |
| `src/staging/validation.py` | `flag_no_rand_spenders(df, raise_or_warn)` | validate no-R&D-spender responses | dataframe + mode | None (raises/warns) | warning/raise | none | none | validation logic | `src/randd_pipeline/checks/staging_checks.py` |

### D. Postcode harmonisation + issue output seams

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|
| `src/staging/postcode_validation.py` | `format_postcodes(postcode)` | canonicalize postcode spacing/case | postcode str | formatted postcode/None | none | none | none | pure business transform | `src/randd_pipeline/domain/staging/postcodes.py` |
| `src/staging/postcode_validation.py` | `create_issue_df(full_df, flagged_df)` | construct invalid-postcode issue rows | full df + flagged index | issue dataframe | none | none | none | pure business transform | `src/randd_pipeline/domain/staging/postcodes.py` |
| `src/staging/postcode_validation.py` | `update_full_responses(df, invalid_df)` | null/remove invalid harmonised postcodes + format cols | full df + invalid issue df | updated df | mutates dataframe | none | none | pure business transform (with mutation) | `src/randd_pipeline/domain/staging/postcodes.py` |
| `src/staging/postcode_validation.py` | `check_pcs_real(df, postcode_masterlist, config)` | membership check against masterlist | validation df, masterlist, config | series of unreal postcodes | none | `global.postcode_csv_check` | none | validation logic | `src/randd_pipeline/checks/staging_postcode_checks.py` |
| `src/staging/postcode_validation.py` | `check_log_unreal_postcodes(...)` | apply format + identify unreal + build issue df | validation df, masterlist, config | `(invalid_postcode_df, unreal_postcodes)` | none | `global.postcode_csv_check` | none | validation logic | split between domain + checks |
| `src/staging/postcode_validation.py` | `run_full_postcode_process(df, postcode_mapper, config)` | wrapper for harmonised postcode column, validation, issue merge, update | full responses df, mapper df, config | `(full_responses, invalid_postcode_df)` | dataframe mutation + logging | global/survey flags indirectly | none | orchestration wrapper | thin asset/helper wrapper later; not first seam |
| `src/staging/staging_helpers.py` | `stage_validate_harmonise_postcodes(...)` | loads mapper, runs postcode process, writes invalid postcode CSV | config + full_responses + file funcs | `(full_responses, postcode_mapper)` | mapper file read + invalid postcode CSV write | `mapping_paths.postcode_mapper`, `staging_paths.pcode_val_path`, `survey.survey_type` | read/write CSV callables | mixed: validation + QA output side effect + file I/O | split: domain/checks + optional export asset; keep wrapper legacy now |

### E. Mapper/manual/backdata load and QA output seams

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|
| `src/staging/staging_helpers.py` | `load_validate_mapper(...)` | load mapper CSV + schema validate + null checks | mapper key, config, logger, read/file funcs, optional cols | mapper dataframe | file read + schema validation + logs | `mapping_paths[...]` | read CSV + exists callable | file/platform I/O + validation logic | thin asset helper in `assets/staging.py` + checks in `checks/`; not domain-first seam |
| `src/staging/staging_helpers.py` | `getmappername(...)` | parse mapper key string | mapper key + split flag | mapper name str | none | none | none | pure utility | `src/randd_pipeline/domain/staging/utils.py` or keep legacy |
| `src/staging/staging_helpers.py` | `filter_pnp_data(full_responses, config)` | survey-type filter BERD vs PNP by legalstatus | dataframe + config | filtered dataframe | none | `survey.survey_type` | none | pure business transform | `src/randd_pipeline/domain/staging/filters.py` |
| `src/staging/staging_helpers.py` | `output_staging_qa(...)` | optional staged-response CSV output | dataframe + config + writer + logger | none | writes QA CSV | `global.output_full_responses`, `global.run_with_frozen_data`, `survey.survey_type`, `staging_paths.staging_output_path` | write CSV callable | QA output side effect | optional `assets/exports.py` later; keep legacy for now |
| `src/staging/staging_helpers.py` | `fix_anon_data(...)` | dev-test anonymised workaround for missing cols | responses df + config | patched df | randomised synthetic fill | `devtest.seltype_list` | none | unclear / needs human decision | likely delete/deprecate later (if no longer required) |

### F. Pipeline integration call site

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|
| `src/pipeline.py` | `run_pipeline(...)` call to `run_staging(...)` | legacy orchestration integration point | full pipeline config and platform module callables | staged tuple passed downstream | legacy runlog + stage sequencing | broad runtime config | platform-specific `mods` functions | orchestration wrapper | keep legacy parity path; do not extend in refoundation |

## 4) Recommended first extraction seam

### Recommendation
Extract the **snapshot transmutation seam** centered on:
- `src/staging/spp_snapshot_processing.py::full_responses`
- with helper usage of `create_contextual_dataframe` and `create_response_dataframe`
- and optionally a companion metric function `response_rate`.

### Why this is the smallest useful seam
- Deterministic pandas-only operations (merge/pivot/drop/astype).
- No dependency on file adapters/network/S3/HDFS.
- No freezing/construction coupling.
- Testable with very small synthetic contributors/responses tables.
- Produces the core staged dataframe shape needed for eventual `intermediate/staged_responses`.

### Explicit non-goals for first seam PR
- Do **not** extract `run_staging` wrapper.
- Do **not** migrate mapper file loading/manual files in first seam.
- Do **not** migrate QA CSV outputs in first seam.
- Do **not** add Dagster asset first; add domain/test seam first.

## 5) Synthetic fixture requirements (minimum)

Proposed new scenario: `tests/fixtures/synthetic/staging_minimal_valid_responses/`

### Input CSVs
1. `contributors.csv`
2. `responses_long.csv`

### Expected output CSVs
1. `expected_full_responses.csv`
2. (optional) `expected_response_rate.csv` with a single scalar value if `response_rate` is included.

### Required columns

#### `contributors.csv`
- `reference`, `instance`, `survey`, `period`
- plus minimal contextual columns retained by staging merge (for example `formtype`, `legalstatus`, `referencepostcode`)
- include dropped metadata columns to exercise drop behavior: `createdby`, `createddate`, `lastupdatedby`

#### `responses_long.csv`
- `reference`, `instance`, `survey`, `period`
- `questioncode`, `response`
- dropped cols to exercise behavior: `createdby`, `createddate`, `lastupdatedby`, `lastupdateddate`, `adjustedresponse`

#### `expected_full_responses.csv`
- legacy input columns (`survey`, `period`) are canonicalised to refoundation output columns (`survey_type`, `survey_year`)
- one row per (`reference`, `instance`, `survey_type`, `survey_year`) as the explicit refoundation target grain
- pivoted question columns (example: `601`, `701`) with values from long responses
- contextual contributor columns retained

### Business rules exercised
- long-to-wide pivot by `questioncode`
- merge contributors/responses on canonical keys `reference/instance/survey_type/survey_year` to avoid ambiguous duplicate instance columns (`instance_x`/`instance_y`)
- dedup contextual rows
- `instance` cast handling in response dataframe

### Why non-sensitive and minimal
- tiny synthetic rows only (e.g., 2 references, 2 question codes)
- no real company identifiers/real postcodes required
- no mapper/manual/backdata files required for this seam
- isolates one deterministic transform contract for parity-first migration

## 6) Future target shape (not implemented here)

Planned staged flow:

`raw/full_responses` (Iceberg: `raw.full_responses`)  
`↓`  
`intermediate/staged_responses` (Iceberg: `intermediate.staged_responses`)

Phased implementation intent:
1. Extract Python domain helper seam first (`domain/staging/*`).
2. Add focused unit tests against tiny synthetic fixture(s).
3. Add thin Dagster asset wrapper in `src/randd_pipeline/assets/staging.py`.
4. Add contract-backed checks in `src/randd_pipeline/checks/`.
5. Compare outputs to legacy oracle path and document reconciliation evidence.

## 7) Risks and open questions

1. **Schema assumptions:** current TOML-driven casting includes coercions and in-place mutation; decision needed on whether this remains or is replaced by contract checks + explicit typed transforms.
2. **Postcode treatment:** current postcode logic combines formatting, source precedence (`601` vs `referencepostcode`), invalid filtering, and output side effects; extraction boundaries need clear split between transform vs QA export.
3. **Mapper loading:** `load_validate_mapper` is I/O-coupled to config paths and schema files; decide whether to stage as explicit raw/ops input tables first.
4. **Freezing/construction leakage:** run flags in `run_staging` still gate behavior (feather shortcuts, snapshot variants); clarify what staging behavior is canonical independent of legacy run modes.
5. **Legacy QA outputs:** invalid postcode CSV and staged output CSV are mixed into stage execution; decide whether these become optional terminal export assets.
6. **Survey-type branching:** `filter_pnp_data` legalstatus branch is simple but business-critical; confirm where this should sit (domain transform vs asset-level branching).
7. **Dev-test anonymised workaround (`fix_anon_data`)**: likely legacy-only; confirm if still needed for any active fixtures.

## Expected numerical equivalence statement for follow-on extraction PR

For the first extraction seam PR (transmutation helpers), expected outcome is:
- **numerical/dataframe equivalence** to legacy transmutation output for the same synthetic inputs,
- with differences treated as defects unless explicitly approved.

Reconciliation should be recorded at row/column grain for `expected_full_responses` fixture outputs.

## 8) Implementation note: first staging transmutation seam

The first refoundation-domain staging seam has now been implemented under
`src/randd_pipeline/domain/staging/transmutation.py` using tiny synthetic fixtures.

This implementation is a clean re-expression of deterministic contributor/response
transmutation behaviour (merge + long-to-wide pivot) and is intentionally **not** a
port of `run_staging`.

For this seam, legacy snapshot input terminology (`survey`, `period`) is treated as
input-shape compatibility only, while refoundation outputs are canonicalised to
`survey_type` and `survey_year`. This naming contract is explicit refoundation
direction rather than blind preservation of legacy column names.

Dagster asset wiring and Iceberg materialisation for staging are intentionally
deferred to follow-on PRs.


## 9) Implementation note: first thin staging asset

A first thin staging smoke asset now exists at `src/randd_pipeline/assets/staging.py` as
`staged_responses` where Dagster asset key `intermediate/staged_responses` maps to Iceberg table identifier `intermediate.staged_responses`.

The asset is intentionally a small wrapper over the clean transmutation seam
(`build_full_responses(...)`) and deliberately does **not** port `run_staging`.

Contract-backed checks for `intermediate.staged_responses` are now implemented as the follow-on thin checks slice.

Contract-backed checks for `intermediate.staged_responses` now exist and validate table existence, non-empty output, required columns from the refoundation table contract, and explicit uniqueness at `reference + instance + survey_type + survey_year`.

# 0007: Mapping seam inventory and extraction plan

- **Status:** Proposed
- **Date:** 2026-05-23
- **Type:** Planning / inventory only (no runtime migration)
- **Depends on:** 0002 migration principles, 0003 refoundation strategy, 0004 legacy deprecation map, 0005 business-logic inventory, 0006 staging seam inventory

## 1) Purpose

This note prepares the next business migration seam before re-expressing mapping behaviour in the lean refoundation package (`src/randd_pipeline/`). It inventories legacy mapping functions at module/function level, classifies each responsibility, and proposes the first small extraction target.

This is explicitly **not**:
- a migration of `run_mapping`,
- a wrapper around the legacy mapping stage,
- or a change to legacy mapping runtime orchestration.

For this phase, legacy mapping code remains a **parity oracle only** while the future refoundation seam is designed.

## 2) Current legacy mapping role

Conceptually, the current mapping stage in `src/mapping/mapping_main.py::run_mapping(...)` does the following:

1. Loads mapper/reference tables (foreign ownership, ITL/geography, cell number, PG conversion, SIC→PG, and optionally 2022 reference-list update input).
2. Validates mapper relationships (null checks via staging helper path, uniqueness checks, and many-to-one checks for PG/SIC mapper pairs).
3. Applies product-group conversion (fill null PG from SIC mapping, then numeric-to-alpha PG conversion).
4. Applies foreign ownership mapping (`ultfoc`) and defaulting for unmapped values.
5. Applies cell-number mapping (`UNI_Count`/`uni_employment` enrichment and range/uniqueness checks).
6. Applies postcode-to-ITL/geography joins for GB responses.
7. Handles PNP-specific area mapping branch (`add_area_column`).
8. Handles NI-specific mapping branch (`join_itl_regions_ni`, `create_additional_ni_cols`).
9. Writes optional mapping QA CSV artefacts for GB and NI outputs.

## 3) Function-level inventory

### A. Orchestration wrapper and stage-level wiring

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target location |
|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/mapping_main.py` | `run_mapping(...)` | Legacy mapping stage wrapper: loads mappers, validates, branches by survey/year, runs joins, writes QA CSVs | `full_responses`, `ni_full_responses`, `postcode_mapper`, `config`, `rd_read_csv`, `rd_write_csv`, `rd_file_exists` | `(mapped_gb_df, mapped_ni_df, itl_mapper)` | mapper CSV reads, QA CSV writes, logging | `mapping_paths.*`, `global.output_mapping_qa`, `global.output_mapping_ni_qa`, `survey.survey_type`, `survey.survey_year`, `mappers.*` | `stage_hlp.load_validate_mapper`, `rd_read_csv`, `rd_file_exists`, `rd_write_csv` | orchestration wrapper | keep legacy only (parity oracle), then deprecate later after seam migration |

### B. Mapper validation and join helper functions

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target location |
|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/mapping_helpers.py` | `mapper_null_checks(...)` | Checks for nulls in selected mapper columns | mapper df, mapper name, optional column list | none | prints warnings | none | none | validation logic | `src/randd_pipeline/checks/` |
| `src/mapping/mapping_helpers.py` | `join_with_null_check(...)` | Left join + detect unmapped keys (`left_only`) | base df, mapper df, mapper name, join column, warn flag | joined df | may raise/warn; adds/drops `_merge` | none | none | reference-data join + validation logic | domain helper in `src/randd_pipeline/domain/mapping/` and check counterpart in `src/randd_pipeline/checks/` |
| `src/mapping/mapping_helpers.py` | `col_validation_checks(...)` | Column type/length/capitalisation validation for mapper columns | mapper df, column name, expected constraints | none | raises on failed checks | none | none | validation logic | `src/randd_pipeline/checks/` |
| `src/mapping/mapping_helpers.py` | `check_mapping_unique(...)` | Asserts uniqueness on mapper key column | mapper df, column name | none | raises on duplicates | none | none | validation logic | `src/randd_pipeline/checks/` |
| `src/mapping/mapping_helpers.py` | `update_ref_list(...)` | 2022-specific reference list correction: set `cellnumber=817`, `selectiontype=L` for matched refs | full responses df, ref list df | updated df | raises if mapper references missing from responses | indirectly year-gated in caller | none | survey-specific branch + pure business transform | likely `src/randd_pipeline/domain/mapping/` (if retained), otherwise keep legacy only pending human decision |

### C. Product-group conversion functions

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target location |
|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/pg_conversion.py` | `sic_to_pg_mapper(...)` | Fill null PG values from SIC→PG mapper | response df, sic mapper, column names | updated df | raises when mapper target values are missing (`nan`) | none | none | pure business transform + reference-data join | `src/randd_pipeline/domain/mapping/` |
| `src/mapping/pg_conversion.py` | `pg_to_pg_mapper(...)` | Convert numeric PG to alpha PG and keep copy in `pg_numeric` | response df, pg mapper, column names | updated df | raises when mapper target values are missing (`nan`) | none | none | pure business transform + reference-data join | `src/randd_pipeline/domain/mapping/` |
| `src/mapping/pg_conversion.py` | `run_pg_conversion(...)` | Wrapper applying both PG mapping steps to GB (+NI if present) | `(gb_df, ni_df)`, PG mappers | `(gb_df, ni_df)` | logging only | none | none | orchestration wrapper over pure transforms | domain helper in `src/randd_pipeline/domain/mapping/` (or decomposed into explicit transforms) |

### D. Foreign ownership mapping

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target location |
|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/ultfoc_mapping.py` | `join_fgn_ownership(...)` | Validate uniqueness and join `ultfoc` mapper to GB; NI defaulting to GB when blank/null | `(gb_df, ni_df)`, ultfoc mapper | `(mapped_gb_df, mapped_ni_df)` | warnings for unmapped references, default fill to `GB` | none | none | reference-data join + survey-specific branch (NI handling) | split: join transform in `src/randd_pipeline/domain/mapping/`; validation/coverage checks in `src/randd_pipeline/checks/` |

### E. Cell-number mapping

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target location |
|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/cellno_mapping.py` | `clean_validate_cellno_mapper(...)` | Enforce uniqueness and range; select/rename mapper columns | cellno mapper df | cleaned mapper df | raises on invalid mapper | none | none | validation logic + reference-data normalization | split between `src/randd_pipeline/domain/mapping/` (normalization) and `src/randd_pipeline/checks/` (validations) |
| `src/mapping/cellno_mapping.py` | `validate_join_cellno_mapper(...)` | Apply cleaned cellno mapper to GB via null-checked join | `(gb_df, ni_df)`, cellno mapper, config | `(gb_df, ni_df)` | may raise/warn via join helper | currently unused `config` arg | none | reference-data join | `src/randd_pipeline/domain/mapping/` with accompanying checks |

### F. Postcode/ITL/geography joins

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target location |
|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/itl_mapping.py` | `join_itl_regions(...)` | Join postcode mapper to derive `itl`, then join ITL mapper to attach geography columns | responses df, postcode mapper, itl mapper, config | mapped responses df | warns on join misses (`warn=True`) | `mappers.gb_itl`, `mappers.geo_cols` | none | reference-data join | `src/randd_pipeline/domain/mapping/` plus join completeness checks in `src/randd_pipeline/checks/` |

### G. PNP-specific branch

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target location |
|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/pnp_mapping.py` | `add_area_column(...)` | Adds `area` bucket from region code map for PNP branch | responses df | updated df | mutates/adds column | applied only when `survey_type == "PNP"` in caller | none | survey-specific branch | `src/randd_pipeline/domain/mapping/` if PNP retained in target scope; otherwise keep legacy only pending decision |

### H. NI-specific branch

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target location |
|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/ni_mapping.py` | `join_itl_regions_ni(...)` | Set fixed NI ITL code, then join ITL mapper for geography columns | NI df, itl mapper, config | mapped NI df | in-place assignment to `itl`, warn-mode join | `mappers.ni_itl`, `mappers.gb_itl`, `mappers.geo_cols` | none | survey-specific branch + reference-data join | likely separate NI domain helper in `src/randd_pipeline/domain/mapping/` if NI retained; otherwise keep legacy only |
| `src/mapping/ni_mapping.py` | `create_additional_ni_cols(...)` | Inject NI-specific constant columns/values (`a_weight`, `g_weight`, `604`, etc.) | NI df | updated NI df | in-place column assignments | none | none | survey-specific branch | unclear / needs human decision (domain helper vs keep legacy only) |

### I. Mapping-related reused helper dependencies outside `src/mapping/`

| File path | Function/class | Current role | Inputs | Outputs | Side effects | Config dependencies | I/O dependencies | Classification | Proposed target location |
|---|---|---|---|---|---|---|---|---|---|
| `src/staging/staging_helpers.py` | `load_validate_mapper(...)` | Generic mapper CSV reader + schema/null checks used by mapping loader path | mapper key, config, logger, file funcs | mapper df | file reads, schema checks, logging | `mapping_paths.*` + config schema pointers | `rd_file_exists`, `rd_read_csv` | file/platform I/O + validation logic | replace with explicit ref-table asset/input readers for refoundation; keep legacy only in parity path |
| `src/staging/validation.py` | `validate_many_to_one(...)` | Validates one-to-many inconsistencies for mapper pairs (used for PG/SIC checks) | mapper df, key/value column names | validation outcome (raise/log) | raises/warns | none | none | validation logic | `src/randd_pipeline/checks/` |
| `src/pipeline.py` | `run_pipeline(...)` call site to `run_mapping(...)` | Legacy orchestration sequencing between construction and imputation | full stage config + stage dataframes | mapped outputs into imputation | runtime orchestration only | broad pipeline config | stage wrappers/file adapters | orchestration wrapper | keep legacy only (parity oracle path), do not extend |

## 4) Recommended first mapping seam

### Recommendation: first seam = **simple foreign ownership join** (`ultfoc`)

Selected initial seam: a narrow re-expression of GB-side foreign ownership mapper application based on `join_fgn_ownership(...)`, excluding NI branch logic for the first step.

### Why this should be first

1. **Deterministic pandas-only transform:** a left join on `reference` plus controlled default fill (`GB`) for blank/null mapped values.
2. **Tiny fixture friendly:** requires one synthetic staged responses table and one tiny mapper table (`reference`↔`ultfoc`), with straightforward expected output assertions.
3. **Minimal config/path coupling:** no file reader/path resolver needed when mapper is provided directly as dataframe input.
4. **No NI dependency for first seam:** NI branch can be deferred while still delivering useful mapped enrichment for eventual `intermediate.mapped_responses`.
5. **No QA CSV dependency:** pure transform + validation behavior can be tested without output side effects.
6. **Good early reconciliation signal:** join-completeness/default-fill counts are easy to compare against legacy behavior.

### Why not first-seam alternatives (for now)

- **Postcode→ITL/geography join:** useful but depends on two mapper tables plus canonical postcode column assumptions and warning-mode nuances.
- **Cell-number mapping:** also viable, but includes mapper-specific range/uniqueness constraints and additional renamed columns.
- **PG conversion:** valuable but entwines two separate mappers and missing-value behaviour across SIC/PG fields.

## 5) Synthetic fixture requirements (minimum for first seam)

Proposed new scenario directory (example naming):

`tests/fixtures/synthetic/scenarios/mapping_foreign_ownership_minimal/`

### Input staged responses CSV
- `staged_responses.csv`
- Minimum columns:
  - `reference`
  - `instance`
  - `survey_type`
  - `survey_year`
  - existing `ultfoc` (optional blank/null values to test overwrite/fill behaviour)
- Tiny row count (e.g., 3–5 references).

### Reference mapper CSV
- `ultfoc_mapper.csv`
- Minimum columns:
  - `ruref` (join key)
  - `ultfoc` (mapped value)
- Include one mapped value, one blank/null mapped value, and (optionally) one unmatched reference to test default behaviour.

### Expected mapped responses CSV
- `expected_mapped_responses.csv`
- Required columns:
  - all input staged columns,
  - resolved `ultfoc` column after mapping/defaulting.

### Business rules exercised
1. Left join from staged responses `reference` to mapper `ruref`.
2. Drop mapper key column after join (`ruref` removed).
3. Default unresolved/blank/null `ultfoc` values to `GB`.
4. Preserve row count and grain at `reference + instance + survey_type + survey_year`.

### Why fixture is minimal and non-sensitive
- Entirely synthetic references and categorical ownership values.
- No postcodes, no real geography identifiers, no enterprise-sensitive fields required.
- Single tiny mapper + single tiny response table is sufficient for first seam behaviour.

## 6) Future target shape (not implemented here)

Target flow for mapping seam rollout:

`intermediate/staged_responses`
`↓`
`intermediate/mapped_responses`

Phased intent:
1. **Python domain/helper seam first** in `src/randd_pipeline/domain/mapping/`.
2. **Tests against tiny synthetic fixtures** (legacy-vs-refoundation where practical).
3. **Thin Dagster asset wrapper second** in `src/randd_pipeline/assets/mapping.py`.
4. **Contract-backed checks third** in `src/randd_pipeline/checks/`.
5. **Legacy parity evidence** documented for each migrated mapping slice.

## 7) Risks and open questions

1. **Mapper schema canonicalisation:** several legacy mappers use inconsistent column naming (`cell_no` vs `cellnumber`, `ruref` vs `reference`, `pcd2` vs `postcodes_harmonised`) and need a stable refoundation contract.
2. **Many-to-one validation policy:** current `validate_many_to_one` behaviour and failure mode should be made explicit as check contracts (error vs warn expectations).
3. **Postcode/ITL geography semantics:** clarify canonical handling of missing postcodes, join warning thresholds, and expected geography column set.
4. **PNP-specific area mapping:** confirm whether static `region→area` mapping remains business-critical and where it belongs in target domain shape.
5. **NI branch treatment:** decide whether NI-specific constants and ITL assignment remain in-scope for lean pipeline or move to a dedicated NI pathway.
6. **2022 reference-list update branch:** confirm whether `update_ref_list` is a one-off historical patch, a recurring rule, or a correction-input candidate.
7. **Reference mappers as explicit tables:** decide which mapper inputs should become explicit `ref.*`/`ops.*` table assets rather than ad hoc CSV loads.
8. **Legacy QA CSV outputs:** determine which mapping QA outputs (if any) remain as optional terminal export assets versus being replaced entirely by asset checks.
9. **Staging naming leakage:** verify whether mapping assumes legacy staging column names that should be canonicalised at the seam boundary (`survey/period` vs `survey_type/survey_year`, postcode column variants).

## Expected numerical equivalence statement for follow-on implementation PR

For each extracted mapping seam, expected behaviour is numerical/row-level equivalence to legacy logic on shared synthetic fixtures at declared grain; any differences are defects unless explicitly approved as intentional design changes.

## 8) Implementation note (PR 027)

The first clean mapping-domain seam is now implemented in `src/randd_pipeline/domain/mapping/foreign_ownership.py` as a pandas-only function operating on provided dataframes.

Scope implemented in this PR:
- GB-side foreign ownership mapping only (`reference` -> `ultfoc` via mapper `ruref`).
- Explicit defaulting to `GB` for missing/blank/null mapped ownership.
- Row-count and declared grain preservation (`reference + instance + survey_type + survey_year`) validated with tiny synthetic fixtures.

Explicitly not implemented in this PR:
- `run_mapping` migration or wrappering.
- NI mapping branch logic.
- PNP area mapping.
- Postcode/ITL mapping.
- Product-group conversion.
- Cell-number mapping.
- Mapper file loading.
- Mapping QA CSV outputs.

## 9) Implementation note (PR 028)

A first thin mapping Dagster smoke asset now exists in `src/randd_pipeline/assets/mapping.py`.

Scope of this asset:
- Wraps the clean foreign ownership seam only (`apply_foreign_ownership_mapping(...)`).
- Reads `intermediate.staged_responses` from `TableStore` and materialises `intermediate.mapped_responses`.

Deliberate non-goals of this PR:
- Does not port or wrap `run_mapping`.
- Does not include postcode/ITL, PG conversion, cell-number, PNP, or NI mapping branches.
- Contract-backed checks for `intermediate.mapped_responses` now exist.
- Checks validate table contract required columns, explicit refoundation grain uniqueness (`reference+instance+survey_type+survey_year`), and populated `ultfoc`.
- Checks deliberately remain narrower than full legacy mapping QA.


## 10) Implementation note (PR 031)

- `ref.ultfoc_mapper` now exists as the first explicit reference-table input for mapping in the lean refoundation slice.
- Current scope supports the foreign ownership seam only (`staged_responses` + `ref.ultfoc_mapper` -> `mapped_responses`).
- General reference-data ingestion and broader mapper-loading frameworks remain out of scope.


## 11) Implementation note (PR 033)

Contract-backed checks now exist for `ref.ultfoc_mapper` (asset key `ref/ultfoc_mapper`) covering:
- table existence,
- non-empty table output,
- required columns from table contract,
- uniqueness at `ruref` grain.

These checks protect the first explicit reference input table for the foreign ownership seam. Blank/null mapper `ultfoc` values remain permitted for now, because the seam currently defaults unresolved/blank/null ownership to `GB`.

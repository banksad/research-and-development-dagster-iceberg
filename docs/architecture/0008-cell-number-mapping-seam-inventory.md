# 0008: Cell-number mapping seam inventory and extraction plan

- **Status:** Proposed
- **Date:** 2026-05-23
- **Type:** Planning / inventory only (no runtime migration)
- **Depends on:** 0002 migration principles, 0003 refoundation strategy, 0004 legacy deprecation map, 0005 business-logic inventory, 0007 mapping seam inventory

## 1) Purpose

This note prepares the next clean mapping-domain seam after the first foreign-ownership seam. It provides a function-level inventory for legacy cell-number mapping and a concrete implementation plan for a small, reversible follow-on PR in `src/randd_pipeline/`.

This is explicitly **not**:
- a migration of `src/mapping/mapping_main.py::run_mapping(...)`,
- a wrapper around legacy mapping orchestration,
- a port of legacy mapper file-loading pathways.

Legacy cell-number mapping remains a **parity oracle only** until a clean seam is implemented and reconciled.

## 2) Current legacy role

Legacy cell-number mapping is currently wired by `run_mapping(...)` as one step in a larger mapping stage:

1. `run_mapping(...)` loads the cell-number mapper through `stage_hlp.load_validate_mapper("cellno_path", ...)`.
2. `run_mapping(...)` calls `validate_join_cellno_mapper(responses, cellno_df, config)` after PG conversion and foreign ownership joins.
3. `validate_join_cellno_mapper(...)` cleans and validates mapper columns/rules with `clean_validate_cellno_mapper(...)`.
4. The cleaned mapper is left-joined to the GB responses dataframe using `join_with_null_check(...)` on `gb_df.cellno == mapper.cellnumber`.
5. NI responses are passed through unchanged in this seam.

Conceptually, the cell-number mapping expects:

- **Input response join key:** `cellno` (GB responses dataframe).
- **Input mapper columns:** `cell_no`, `UNI_Count`, `uni_employment`.
- **Cleaning/renaming behavior:**
  - retain only `cell_no`, `UNI_Count`, `uni_employment`,
  - rename `cell_no -> cellnumber`,
  - rename `UNI_Count -> uni_count`.
- **Range checks:** `cell_no` must be within inclusive range `1..817`.
- **Uniqueness checks:** `cell_no` must be unique.
- **Join behavior:** left join on response `cellno` to mapper `cellnumber`; fail if non-null response keys do not match mapper rows.
- **Output columns added:** `cellnumber`, `uni_count`, `uni_employment` on GB output rows.
- **Config dependency:** `config` argument is accepted by `validate_join_cellno_mapper(...)` but is currently unused inside this function.
- **Warning/error behavior:**
  - uniqueness/range failures raise `ValueError`,
  - join misses raise `ValueError` by default (`warn=False` path in `join_with_null_check(...)`).

## 3) Function-level inventory

### A. Cell-number specific functions

| File path | Function | Current role | Inputs | Outputs | Side effects | Validation rules | Config deps | I/O deps | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/cellno_mapping.py` | `clean_validate_cellno_mapper(cellno_df)` | Canonicalise and validate cell-number mapper | mapper dataframe with `cell_no`, `UNI_Count`, `uni_employment` | cleaned mapper with `cellnumber`, `uni_count`, `uni_employment` | none | `cell_no` unique; `cell_no` between 1 and 817; required columns implied by selection | none | none | mapper normalisation + validation logic | split into domain normaliser in `src/randd_pipeline/domain/mapping/` + explicit checks in `src/randd_pipeline/checks/` |
| `src/mapping/cellno_mapping.py` | `validate_join_cellno_mapper(responses, cellno_df, config)` | Apply validated cell-number mapper to GB dataframe in responses tuple | `(gb_df, ni_df)` tuple; raw mapper df; config dict | updated `(gb_df, ni_df)` tuple | none | uses `clean_validate_cellno_mapper(...)`; join completeness validation via helper | accepts `config`, currently unused | none | reference-data join + orchestration wrapper | replace with pandas-only domain seam function; remove unused config from new function signature |

### B. Helper dependencies used by cell-number mapping

| File path | Function | Current role in this seam | Inputs | Outputs | Side effects | Validation rules | Config deps | I/O deps | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/mapping_helpers.py` | `check_mapping_unique(mapper_df, col_to_check)` | Enforce unique mapper keys before join | mapper df, column name | none | none | raises if key not unique | none | none | validation logic | check utility under `src/randd_pipeline/checks/` |
| `src/mapping/mapping_helpers.py` | `join_with_null_check(df, mapper_df, mapper_name, join_col, warn=False)` | Left join + unmatched-key guardrail for mapper application | base df, mapper df, mapper label, join col | joined df | may log warning or raise error; temporary `_merge` column dropped | identifies `left_only` rows where join key was non-null and unmatched | none | none | reference-data join + validation logic | domain helper in `src/randd_pipeline/domain/mapping/` with complementary check behaviour in `src/randd_pipeline/checks/` |

### C. Wiring/orchestration context (for parity reference only)

| File path | Function | Current role in wiring cell-number seam | Inputs | Outputs | Side effects | Validation rules | Config deps | I/O deps | Classification | Proposed target |
|---|---|---|---|---|---|---|---|---|---|---|
| `src/mapping/mapping_main.py` | `run_mapping(...)` | Loads mapper (`cellno_path`) and invokes cell-number join in wider legacy stage | full responses, NI responses, mappers/config, file adapters | mapped tuples + itl mapper | file reads, optional QA writes, logging | indirect via called functions | heavy config use | legacy CSV/file adapter stack | orchestration wrapper | keep legacy only (parity oracle), do not migrate as seam |

## 4) Recommended first implementation seam

### Recommendation

Implement the smallest useful clean seam as a **single pandas-only function** in `src/randd_pipeline/domain/mapping/` that:

- takes a mapped/staged responses dataframe (GB-shaped rows) and a cell-number mapper dataframe,
- normalises a tiny canonical mapper shape,
- validates key invariants,
- left-joins mapper metadata onto responses,
- preserves row count and declared grain.

### Suggested seam contract (function-level)

Proposed function shape for follow-on PR (name illustrative):

- `apply_cell_number_mapping(responses_df: pd.DataFrame, cell_number_mapper_df: pd.DataFrame) -> pd.DataFrame`

Expected behaviour:

1. Require response join key (`cellno`) and required mapper columns.
2. Canonicalise mapper to `cellnumber`, `uni_count`, `uni_employment`.
3. Enforce mapper key uniqueness and mapper key range (`1..817`) to match legacy behaviour.
4. Perform left join from `cellno` -> `cellnumber`.
5. Fail on unmatched non-null `cellno` values (legacy-equivalent strictness).
6. Preserve input row count and grain (`reference + instance + survey_type + survey_year`).

Why this seam first:

- It is isolated from `run_mapping(...)` orchestration.
- It has clear deterministic behaviour.
- It requires one mapper table only.
- It can be validated with a tiny synthetic fixture and parity checks.

## 5) Proposed ref table contract

### Table identifier

- `ref.cell_number_mapper`

### Minimal column contract

Required columns:

1. `cellnumber` (integer mapper key)
2. `uni_count` (numeric/integer universe count)
3. `uni_employment` (numeric/integer universe employment)

### Grain and key expectations

- Expected grain: one row per `cellnumber`.
- Primary key expectation: `cellnumber` unique and non-null.

### Value/quality expectations

- `cellnumber` range check: inclusive `1..817` (legacy-equivalent baseline).
- `uni_count` and `uni_employment` may be nullable only if explicitly approved; default expectation should be non-null for reliable downstream use.
- Checks should enforce:
  - required columns,
  - unique key,
  - key range,
  - optional non-null constraints for measure columns (final decision in implementation PR).

## 6) Synthetic fixture requirements

Scenario directory now implemented:

- `tests/fixtures/synthetic/scenarios/mapping_cell_number_minimal/`

Proposed fixture files:

1. `mapped_or_staged_responses.csv`
   - Minimum columns: `reference`, `instance`, `survey_type`, `survey_year`, `cellno`.
2. `cell_number_mapper.csv`
   - Minimum columns aligned to target contract: `cellnumber`, `uni_count`, `uni_employment`.
3. `expected_mapped_responses.csv`
   - Includes input columns plus joined `cellnumber`, `uni_count`, `uni_employment`.

Cases to cover with tiny synthetic data:

- Valid matched `cellno` rows.
- Null `cellno` row that should remain unmapped without failure.
- At least one unmatched non-null `cellno` row for strict-failure path test.
- Optional dedicated invalid-mapper case (duplicate key or out-of-range key) for validation tests.

Why this fixture is minimal/non-sensitive:

- Contains only synthetic identifiers and numeric metadata.
- No personal, enterprise, postcode, or production-linked fields.
- Small row count with explicit expected outcomes for parity-style reconciliation.

## 7) Target asset/check rollout (future sequence, not implemented here)

1. **Domain function**
   - Add pandas-only cell-number seam under `src/randd_pipeline/domain/mapping/`.
2. **Ref table asset**
   - Add thin `ref/cell_number_mapper` asset materialising `ref.cell_number_mapper`.
3. **Mapped output wiring**
   - Either enhance existing `mapped_responses` seam or add a separate intermediate seam asset for cell-number enrichment.
4. **Checks**
   - Add ref-table contract checks (required cols/unique/range).
   - Add mapped-output checks for row-count/grain preservation and expected mapped column presence.
5. **Chain smoke update**
   - Extend staging-to-mapping smoke path to include cell-number seam behavior with synthetic fixture evidence.

## 8) Risks and open questions

1. **Canonical naming boundary:** should runtime contracts standardise on `cellnumber` only, with legacy `cell_no` accepted only at ingestion/normalisation boundaries?
2. **Graph order:** should cell-number mapping execute before or after foreign ownership in target mapping graph, or is order-independent for current logic?
3. **Column strategy:** should seam add new mapped columns only, or overwrite/prefer existing similarly named columns when present?
4. **Missing mapper rows:** should unmatched non-null `cellno` remain hard-error (legacy-equivalent) or become warn/check-fail policy?
5. **Range policy:** keep strict `1..817` as long-term rule, or externalise threshold to table contract/config?
6. **Unused config argument:** legacy `validate_join_cellno_mapper(..., config)` does not use `config`; confirm target seam intentionally omits config dependency.
7. **Intermediate table shape:** should cell-number enrichment remain in `intermediate.mapped_responses` or move toward one-asset-per-mapping-seam composition?

## Expected numerical equivalence statement for follow-on implementation PR

For the first implemented cell-number seam, expected behaviour is row-level and value-level equivalence to legacy cell-number mapping at declared grain on shared synthetic fixtures. Any differences should be treated as defects unless explicitly approved as intentional changes.


## Implementation note (May 2026)

- The first clean cell-number mapping seam is now implemented in `src/randd_pipeline/domain/mapping/cell_number.py` with strict mapper canonicalisation and join validation.
- A new explicit `ref/cell_number_mapper` asset materialises `ref.cell_number_mapper` by loading legacy-shaped CSV columns (`cell_no`, `UNI_Count`, `uni_employment`) and canonicalising to (`cellnumber`, `uni_count`, `uni_employment`).
- A new `intermediate/cell_number_mapped_responses` asset materialises `intermediate.cell_number_mapped_responses` from `intermediate.mapped_responses` plus the canonical cell-number mapper table.
- This remains a narrow seam implementation and is not a migration of `run_mapping(...)` or full legacy mapping stage port.

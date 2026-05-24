# ADR 0016: Site apportionment seam inventory and first clean implementation plan

- **Status:** Proposed
- **Date:** 2026-05-24

## 1. Purpose

This ADR provides a focused inventory and implementation plan for the **first clean seam** in site apportionment after the currently implemented production-facing refoundation chain through:

- `intermediate.estimated_responses`

This is planning/inventory only.

This ADR is explicitly:

- **not** a migration of the legacy site apportionment runner;
- **not** a wrapper around legacy orchestration patterns;
- **not** a runtime implementation PR;
- **not** a change to estimation, outliers, mapping, imputation, staging, or outputs.

Legacy site apportionment code is treated as a **parity/reference oracle only** during migration. The target production input is canonical `intermediate.estimated_responses`.

## 2. Current legacy role (conceptual)

### 2.1 Where it runs in legacy flow

In `src/pipeline.py`, site apportionment runs after estimation for BERD (and after direct pass-through for PNP), then feeds outputs.

### 2.2 Inputs consumed

The legacy stage consumes a single combined response dataframe containing:

- response keys (`reference`, `period`, `instance`);
- form-type and status/imputation markers (`formtype`, `status`, `imp_marker`);
- site fields (`601` postcode, `602` percentage, `postcodes_harmonised`);
- product/category columns (`201`, `200`, `pg_numeric`);
- mapped geography (`itl` plus configured geo columns);
- weighted/estimated value columns (from `get_imputation_cols(config)`);
- estimation weights (`a_weight`, `g_weight`) and intramural value (`211`) for QA totals.

It also consumes config switches and path settings.

### 2.3 Outputs produced

Legacy site apportionment returns:

- an apportioned response dataframe (same broad schema shape, with rows expanded/split across sites);
- a dictionary of intramural totals before/after apportionment for QA bookkeeping.

Optional side-output files:

- status-filtered QA CSV;
- apportionment QA CSV (`estimated_apportioned` file naming path).

### 2.4 Grain behaviour

Conceptually, legacy behaviour moves from response-level product values to site-level allocation by:

- identifying long-form records with site evidence;
- building a site table per `reference + period`;
- building a category/value table per `reference + period + product/civdef/pg_numeric`;
- forming a Cartesian product and weighting values by site proportions.

This can expand one response/category record into multiple site rows.

### 2.5 Site keys, factors, and data dependencies

Legacy logic depends primarily on:

- site keys: `reference`, `period`, `instance`, postcode (`601` / `postcodes_harmonised`);
- site proportion field: `602`;
- long vs short form branching via `formtype`;
- imputation marker filtering (`R`, `TMI`, `CF`, `MoR`, `constructed` kept);
- geography columns (`itl` + `config["mappers"]["geo_cols"]`);
- value columns inferred from imputation config (`get_imputation_cols(config)`).

There is no dedicated standalone local-unit input table in this stage; it relies on columns already present on the working dataframe.

### 2.6 Survey branches and config dependence

- PNP: weights are force-set (`a_weight = 1.0`, `g_weight = 1.0`) before apportionment.
- BERD: normal outlier/estimation path precedes apportionment.
- NI-specific behaviour is not a separate branch in site apportionment modules; any NI handling is upstream.

Key config dependencies include:

- `survey.survey_type`;
- `apportionment_paths.qa_path`;
- `global.output_status_filtered`;
- `global.output_apportionment_qa`;
- `outputs_paths.outputs_master`;
- `mappers.geo_cols`;
- value-column selection implied by imputation config.

## 3. Function-level inventory

| File path | Function | Current role | Inputs | Outputs | Side effects | Validation rules | Config deps | I/O deps | Type | Proposed target |
|---|---|---|---|---|---|---|---|---|---|---|
| `src/site_apportionment/site_apportionment_main.py` | `run_site_apportionment` | Orchestrates full stage, optional QA outputs, pre/post intram totals | df, config, write_csv | apportioned df + intram totals dict | CSV writes, logging, mutates PNP weights | implicit via downstream ops | survey type, output toggles, paths | `write_csv`, filename helper | mixed orchestration + I/O + transform | keep legacy only; future lean asset wrapper in `src/randd_pipeline/assets/` |
| `src/site_apportionment/site_apportionment.py` | `run_apportion_sites` | Main apportionment transformation pipeline | df, marker list, config, intram dict | apportioned df | prints QA diff, logs | implicit assumptions on required cols | `mappers.geo_cols` and imputation-derived value cols | none direct | mixed (mostly transform, some orchestration) | split into domain functions under `src/randd_pipeline/domain/site_apportionment/` |
| `src/site_apportionment/site_apportionment.py` | `set_percentages` | Normalises/infers percentages for short/single/imputed long forms | df | df with filled `602`/postcode_count | in-place updates | expects form/site columns present | none direct | none | business logic pandas | domain extraction candidate |
| `src/site_apportionment/site_apportionment.py` | `count_unique_postcodes_in_col` | Adds unique postcode count per response key | df | df + `601_count` | none | postcode string length assumptions | none | none | pandas utility | domain extraction candidate |
| `src/site_apportionment/site_apportionment.py` | `split_dataframes` | Separates rows to apportion vs passthrough | df, marker list | to_apportion df, passthrough df | marker filtering | form/site count conditions | none | none | business rule transform | domain extraction candidate |
| `src/site_apportionment/site_apportionment.py` | `create_category_df` | Creates de-duplicated category/value frame | df + column lists | category df | marker filtering | non-null code checks | geo cols / value cols passed in | none | business logic pandas | domain extraction candidate |
| `src/site_apportionment/site_apportionment.py` | `create_sites_df` | Creates de-duplicated site frame | df + cols | sites df | duplicate-site logging | removes blank postcodes | geo cols passed in | none | business logic pandas | domain extraction candidate |
| `src/site_apportionment/site_apportionment.py` | `calc_weights_for_sites` | Converts percentages to normalized site weights | sites df | sites df + `site_weight` | drops temp cols | drops zero-total groups | none | none | pure pandas/stat logic | domain extraction candidate |
| `src/site_apportionment/site_apportionment.py` | `create_cartesian_product` | Expands categories across sites | sites df, category df | merged expanded df | none | relies on join keys existing | none | none | pure pandas transform | domain extraction candidate |
| `src/site_apportionment/site_apportionment.py` | `weight_values` | Applies site weights to numeric columns | df, value cols, weight col | weighted df | in-place multiplication | assumes numeric cols valid | none | none | pure pandas/stat logic | domain extraction candidate |
| `src/site_apportionment/site_apportionment.py` | `sort_rows_order_cols` | Restores/filters column order and sort | df, original cols | sorted df | drops extra cols | none explicit | none | none | transform utility | domain/helper extraction candidate |
| `src/site_apportionment/site_apportionment.py` | `consistency_checks` | Reports weighted intram difference | df, intram dict | none | `print` side effect | none enforced | none | none | QA diagnostic helper | replace with asset check + metadata |
| `src/site_apportionment/output_status_filtered.py` | `output_status_filtered` | Writes filtered-out marker QA file | df, markers, config, write_csv | none | CSV write + logging | none | output paths/toggles | `write_csv`, filename helper | file I/O orchestration | replace with check/QA asset; keep legacy for oracle |
| `src/site_apportionment/output_status_filtered.py` | `calc_weighted_intram_tot` | Calculates pre/post weighted intram totals | df, markers, dict | updated dict | none | marker filter assumptions | none | none | QA/stat utility | domain QA helper or check metadata producer |
| `src/site_apportionment/output_status_filtered.py` | `keep_good_markers` / `save_removed_markers` | Marker inclusion/exclusion filters | df, markers | filtered df | none | imp_marker required | none | none | pure pandas utility | domain/check helper (or shared utility) |
| `src/imputation/imputation_helpers.py` | `get_imputation_cols` | Supplies value-column list used for apportionment weighting | config | list[str] | none | config structure assumptions | imputation/breakdown config | none | config helper | replace via explicit contract columns in lean layer |

## 4. Business-logic vs legacy-plumbing classification

### 4.1 Keep/extract statistical or business logic

- percentage normalisation and inference rules (`set_percentages`);
- site deduplication and weight normalisation (`create_sites_df`, `calc_weights_for_sites`);
- one-to-many expansion and weighted value allocation (`create_cartesian_product`, `weight_values`);
- core split between apportioned and passthrough populations.

### 4.2 Rewrite as clean domain functions

- refactor `run_apportion_sites` into small explicit pandas functions with contract-first signatures;
- remove implicit global column assumptions by requiring explicit key/value column arguments;
- separate value-allocation arithmetic from stage orchestration and QA output concerns.

### 4.3 Replace with Dagster assets/checks

- replace `run_site_apportionment` orchestration wrapper with lean Dagster asset in `src/randd_pipeline/assets/`;
- replace `consistency_checks` print diagnostics with asset checks + materialization metadata;
- replace status-filtered/apportionment QA files with check outcomes and (if needed) explicit QA tables.

### 4.4 Replace with explicit Iceberg input tables

Potential explicit inputs (if/when needed beyond canonical estimated responses):

- `ref.site_apportionment_factors` (explicit site proportions by response/site key);
- `ref.local_units` (if local-unit/site mapping becomes required);
- `ops.site_overrides` (operator-applied site-level corrections or method overrides).

### 4.5 Delete/deprecate legacy file/config plumbing

- path-based QA CSV outputs (`apportionment_paths`, `outputs_paths` routing);
- global output toggles that exist only to write side-effect files;
- implicit stage-level logging/print diagnostics as primary QA evidence.

## 5. Recommended first clean seam

## Recommendation: **minimal site split using explicit site proportions**

Smallest useful next seam:

- consume canonical `intermediate.estimated_responses`;
- join with explicit synthetic site factors (`ref.site_apportionment_factors`) keyed by response identity;
- perform one-record-to-many-site expansion and proportional allocation for a minimal value subset;
- output `intermediate.site_apportioned_responses`.

Why this seam first:

1. It proves the essential analytical behaviour of site apportionment (row expansion + weighted allocation).
2. It avoids pulling in full legacy coupling around postcode inference, marker filtering side-outputs, and path-based QA files.
3. It enables tiny deterministic synthetic tests.
4. It is pandas-only in domain code and cleanly separated from orchestration.
5. It gives an implementation-ready base for later incrementally adding complex legacy edge cases.

If legacy coupling appears higher than expected, fallback should be a narrower spike limited to contract and key semantics only; current inventory suggests the explicit-factor seam is directly implementable next.

## 6. Proposed table contracts (documented only)

### 6.1 `intermediate.site_apportioned_responses` (proposed)

**Proposed grain:**

- response grain + site grain, e.g. `survey_year + survey_type + reference + period + instance + site_id + product key(s)`.

**Likely required columns:**

- carry-through identity: `survey_year`, `survey_type`, `reference`, `period`, `instance`;
- site identity: `site_id` (or canonical site key), optional `site_postcode`;
- apportionment factor columns: `site_proportion`, optional `site_weight`;
- business classification carried from estimated responses (e.g., product/civdef/pg keys);
- apportioned numeric values (selected estimated numeric columns);
- provenance fields: `apportionment_method`, `apportionment_reason` (nullable), `factor_source`.

### 6.2 Conceptual input contracts (future)

- `ref.site_apportionment_factors`
  - keys to response/site grain;
  - factor column (e.g., `site_proportion`);
  - optional effective period or priority fields.
- `ref.local_units` (optional later)
  - site/reference mapping and geography enrichment if needed.
- `ops.site_overrides` (optional later)
  - explicit operator adjustments with reason/provenance.

No table contract files are changed in this ADR-only PR.

## 7. Synthetic fixture requirements (future, not implemented here)

Proposed future fixture scenario path:

- `tests/fixtures/synthetic/scenarios/estimation_to_site_apportionment_minimal/`

Proposed files:

- `input/intermediate__estimated_responses.csv`
- `input/ref__site_apportionment_factors.csv`
- `expected/intermediate__site_apportioned_responses.csv`

Proposed minimal cases:

1. one enterprise/response record split across two sites with proportions `0.6/0.4`;
2. one enterprise/response record with one site at `1.0`;
3. one missing/unmatched factor case (for check behaviour definition);
4. preservation of `survey_year`, `survey_type`, `reference`, `instance` in output;
5. introduction and population of `site_id`.

Why minimal and safe:

- purely synthetic toy values;
- no production identifiers or sensitive content;
- explicit deterministic expected outputs for parity and regression checks.

## 8. Target asset/check rollout (future sequence)

1. Add domain function(s) in `src/randd_pipeline/domain/site_apportionment/` for explicit-factor expansion/allocation.
2. Introduce minimal reference input table contract for factors (if required by seam).
3. Add asset that reads `intermediate.estimated_responses` (+ factor table), writes `intermediate.site_apportioned_responses`.
4. Update table contracts for output and any required inputs.
5. Add site-apportionment asset checks.
6. Add chain smoke test: estimation -> site apportionment using synthetic scenario.

## 9. Operator-facing check ideas (plain language)

Suggested initial check names:

- **Site Apportionment - Required Columns Present**
- **Site Apportionment - Output Not Empty**
- **Site Apportionment - Unique Site Grain**
- **Site Factors - Coverage for Estimated Responses**
- **Site Factors - Proportions Sum To One Per Response**
- **Site Apportionment - No Negative Apportioned Values**
- **Site Apportionment - Site Identifier Populated**
- **Site Apportionment - Method/Reason Populated Where Required**

Expected intent:

- schema and grain integrity are blocking candidates;
- factor completeness and sum-to-one are likely blocking for production-like runs;
- method/reason completeness can begin as warning/review then tighten later.

## 10. Dagster-native design considerations (ADR 0014 alignment)

### 10.1 What should be Dagster Config

- operator-facing scope parameters only (e.g., survey year/type filters, run purpose);
- optional safe feature toggles for strictness policy (e.g., fail on missing factors).

Do not keep legacy file-path QA toggles as operator config.

### 10.2 What should be a Resource

- table/lakehouse access boundaries should stay behind `TableStoreResource` (or successor);
- no direct file-path writes from asset code.

### 10.3 What should be an asset check

- required columns;
- non-empty output;
- unique output grain;
- factor coverage and per-response factor sum checks;
- non-negative apportioned value checks.

### 10.4 Materialization metadata to attach

- row count and output grain cardinality;
- count of split responses and total generated site rows;
- count of missing-factor responses;
- min/max/mean site-factor sums per response;
- survey year/type context.

### 10.5 Likely blocking checks later

Likely blocking in production-like policy:

- missing required columns;
- duplicate site grain rows;
- missing required site factors;
- factor sums outside tolerance;
- invalid negative apportioned totals.

### 10.6 Partitioning implications

- partitioning by `survey_year` (and likely `survey_type`) is plausible;
- partition strategy should be chosen before broader end-to-end rollout to avoid costly reshaping later.

## 11. Risks and open questions

- **Statistical correctness risk:** preserving legacy apportionment semantics while simplifying interfaces.
- **Parity/equivalence risk:** hidden legacy assumptions (especially percentage filling and marker filtering).
- **Hidden dependency risk:** implicit column dependencies currently inherited from monolithic dataframe state.
- **Reference quality risk:** incomplete or inconsistent site factors/local-unit data.
- **Key ambiguity risk:** uncertainty around canonical site identifier (`instance`, postcode, local-unit id, or composite).
- **Survey branching risk:** BERD/PNP differences may need explicit policy in new contracts.
- **Output grain ambiguity:** whether product-level keys must always remain in grain for v1.
- **Scope risk for v1:** whether site apportionment is required for minimal v1 output or can be deferred behind estimation.
- **Evidence risk:** need synthetic + potential golden comparisons against legacy outputs for equivalence confidence.

## 12. Acceptance criteria

- Documentation-only PR.
- New ADR exists at `docs/architecture/0016-site-apportionment-seam-inventory.md`.
- Identifies relevant legacy site apportionment modules/functions.
- Separates statistical/business logic from orchestration/I/O/config plumbing.
- Recommends first clean seam (or explicit smaller spike rationale).
- Proposes future fixture, asset, contract, and check strategy.
- Applies Dagster-native guidance from ADR 0014.
- No runtime code changes.
- No tests changed.
- No assets/checks/contracts changed.
- Open as draft PR against `develop`.


## Implementation update: explicit-factor seam landed
- The first explicit-factor seam now consumes `intermediate.estimated_responses` plus `ref.site_apportionment_factors` and materialises `intermediate.site_apportioned_responses` at response+site grain.
- Included scope: deterministic one-to-many expansion by site factors and proportional value apportionment.
- Excluded scope: legacy percentage inference, postcode/local-unit handling, product/category grain, QA/diagnostic tables, and final outputs.

# ADR 0014: Dagster-native feature alignment for lean refoundation

- **Status:** Proposed
- **Date:** 2026-05-24

## 1. Purpose

This ADR sets a design guardrail for the refoundation: use Dagster-native concepts before creating custom framework code.

- Dagster is the production control plane for the lean refoundation.
- Production operators are expected to use Dagster UI and Launchpad for routine operation.
- Dagster assets, checks, config, resources, metadata, run history, and eventually partitions/schedules are product features, not incidental implementation details.
- This ADR is guidance only; it does not implement new runtime features in this change.

## 2. Current Dagster usage in this repository

Current refoundation usage already includes:

- Dagster assets for raw, staged, mapped, and imputed tables.
- Dagster asset checks for table existence, required columns, grain uniqueness, mapper coverage, imputation markers, and illegal missing values.
- `TableStoreResource` as the table persistence and table-access boundary.
- `SimpleTmiImputationConfig` as the first operator-facing Dagster `Config` class.
- `src/randd_pipeline/definitions.py` as the lean refoundation Dagster entry point.

## 3. Dagster Config / Pythonic config

For operator-facing run parameters:

- Use Dagster `Config` classes.
- Prefer typed fields with field descriptions.
- Prefer validation that fails before run execution where possible.
- Keep Launchpad field names and descriptions understandable for non-programmer production operators.

Examples of parameters that should be operator-facing when needed:

- survey year / period;
- run purpose;
- imputation parameters;
- future outlier/estimation parameters where appropriate.

Avoid:

- unvalidated loose YAML for operator-facing parameters;
- hidden constants inside assets where operators need controlled overrides.

## 4. Resources / `ConfigurableResource`

For runtime integration boundaries:

- Use Dagster resources for external systems and integration points.
- Keep table/lakehouse access behind `TableStoreResource` (or a future production equivalent).
- Configure future GCP/lakehouse/Iceberg service access via resources.
- Assets should not directly instantiate PyIceberg, GCP, or HTTP clients unless there is explicit justification.
- Do not place secrets in code or committed configuration.

## 5. Asset checks

For contracts and quality gates:

- Use asset checks for table contracts and data-quality gates.
- Keep checks visible and understandable in Dagster UI.
- Ensure checks state what failed and why it matters.

Current and near-term examples include:

- required columns;
- non-empty outputs;
- unique response grain;
- mapper coverage;
- imputation marker completeness;
- illegal missing imputed values;
- future reconciliation checks.

## 6. Blocking checks and warning/review checks (future policy)

Design direction for production-like operation:

- Blocking checks should prevent unsafe downstream materialisation.
- Likely blocking failures include schema/key/reference failures and illegal missing values.
- Warning/review checks should surface unusual but still possible conditions.
- Exact blocking-vs-warning policy should be decided before production-like v1.

Illustrative split:

- **Blocking:** missing required columns, duplicate response grain, invalid reference mapper keys.
- **Warning/review:** unusual imputation rate, large movement from prior run, high correction volume.

## 7. Asset metadata and tags

Use Dagster metadata and tags to improve debugging and operator confidence.

- Prefer Dagster metadata/run history over recreating legacy runlog behaviour.
- Attach operationally useful context directly to materialisations/checks where practical.

Useful metadata/tag examples:

- table identifier;
- row count;
- column count;
- survey year / period;
- imputation count;
- no-mean-found count;
- warning count;
- future Iceberg snapshot IDs;
- future correction input snapshot IDs;
- future release/publication tag.

## 8. Partitions and backfills (future decision)

Likely future choices:

- probable partition dimensions include survey year and survey type;
- partitions may support rerun/backfill for a specific year/type without custom run-mode flags;
- partition strategy should be decided before the full end-to-end synthetic v1 becomes too large.

This ADR does not implement partitions in this PR.

## 9. Schedules and sensors (future)

Likely future operation model:

- schedules may support regular production windows;
- sensors may react to upstream/lakehouse availability;
- do not build a custom scheduler when Dagster-native scheduling/sensors fit.

This ADR does not implement schedules or sensors in this PR.

## 10. I/O managers (deferred)

Current position:

- explicit `TableStoreResource` boundaries are acceptable while access patterns stabilise;
- an Iceberg-backed I/O manager could reduce boilerplate later;
- do not introduce an I/O manager prematurely;
- revisit after more stages share the same read-transform-write table pattern.

## 11. Check factories / reusable check helpers (future)

Current repeated check patterns include `table_exists`, `non_empty`, `required_columns`, and `unique_grain`.

Guidance:

- consider check factories/helper builders once patterns stabilise across more stages;
- avoid over-abstraction while pipeline shape is still evolving.

Implementation note (current state):

- repeated dataframe-level checks are now backed by small internal helper functions in `src/randd_pipeline/checks/common.py`;
- helper scope is intentionally lightweight and explicit rather than a large abstraction layer;
- no external data-validation framework has been introduced;
- Dagster asset checks remain the operator-facing quality mechanism.

## 12. Features not to invent ourselves unless needed

Prefer Dagster-native capabilities before custom frameworks. Explicitly avoid inventing custom equivalents for:

- custom run logs;
- custom scheduler;
- custom QA dashboard;
- custom operator config parser;
- custom stage orchestration;
- hidden file-path registries;
- custom lineage layer where Dagster + Iceberg metadata provides equivalent evidence.

## 13. Other FOSS tools

Balanced position:

- other FOSS tools may be useful later for validation, data diffing, metadata cataloguing, observability, documentation, or table-quality checks;
- near-term implementation should stay Dagster-native because Dagster already covers core production-operation needs;
- do not add another framework unless there is a specific gap, clear owner, small proof of value, and no simpler Dagster-native route.

This section names categories only and does not add dependencies.

## 14. Near-term implementation implications

For upcoming refoundation work:

- use Dagster `Config` for future operator-facing stage configuration;
- continue using `TableStoreResource` for table access boundaries;
- add meaningful metadata to future asset/check outputs;
- decide blocking-vs-warning check policy before production-like v1;
- consider partition strategy before the full end-to-end synthetic run is large;
- defer I/O manager introduction and extra FOSS tooling until current patterns stabilise.

## 15. Anti-goals

This ADR does **not**:

- implement partitions;
- implement schedules/sensors;
- introduce an I/O manager;
- add new dependencies;
- deploy to GCP;
- expand imputation/statistical logic.

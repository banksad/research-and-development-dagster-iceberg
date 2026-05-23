# Table contracts (lean Dagster/Iceberg refoundation)

This directory defines scaffolding contracts for canonical Iceberg tables in the target lean architecture.

## Intent
- Make table identifiers explicit and reviewable.
- Provide conservative schema/grain metadata for orchestration and validation planning.
- Keep contracts lightweight during early migration.

## Scope and migration status
- These contracts are scaffolding for target-state table definitions.
- They intentionally do **not** implement Iceberg read/write behavior.
- They intentionally avoid inventing unknown statistical schemas.

## Corrections policy
- `ops.response_corrections` and `ops.postcode_corrections` are explicit, auditable correction inputs.
- These replace the legacy hidden construction-mutation pattern.
- All correction records should be attributable via source, approvals, and timestamps.

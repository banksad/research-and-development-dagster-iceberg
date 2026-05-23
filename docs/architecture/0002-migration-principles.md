# 0002: Migration principles for lean Dagster + Iceberg refoundation

- **Status:** Accepted
- **Date:** 2026-05-23

## Purpose
Define implementation rules for migrating from the current file-era pipeline to the target lean Dagster/Iceberg architecture.

## Principles
1. **No unrequested logic rewrites**
   - Do not change validated statistical/business logic unless explicitly requested.
2. **Analytical meaning over legacy plumbing**
   - Preserve analytical intent; deprecate legacy run-mode/path abstractions that only serve file-era orchestration.
3. **Dagster/Iceberg as first-class runtime**
   - Dagster assets/checks are orchestration/QA; Iceberg tables are canonical persistence.
4. **Parity-first migration safety**
   - Keep the existing pathway only as a temporary oracle for equivalence checks.
5. **Small, reversible, sequenced PRs**
   - Scope PRs narrowly, include rollback approach, and avoid broad cross-cutting rewrites.
6. **Explicit corrections, not hidden mutation**
   - Represent amendments as explicit input assets/tables (for example `ops.response_corrections`, `ops.postcode_corrections`).
7. **Legacy freezing/construction deprecation**
   - Replace freezing with Iceberg snapshot/history/metadata + explicit release tagging.
   - Replace construction with explicit correction datasets and auditable lineage.

## Required PR declarations
For migration-related PRs, include:
- whether runtime Python logic changed (default: no),
- assumptions and risks,
- rollback notes,
- relevant tests/checks run,
- expected numerical equivalence statement,
- reconciliation evidence for changed pathways.

## Parity and reconciliation policy
For each changed pathway:
- define comparison grain (table-level and key metrics),
- provide evidence from checks or fixtures,
- identify any differences as expected vs defects.

## Data handling constraints
- Never use production data, secrets, or credentials.
- Prefer synthetic/test fixtures for migration and reconciliation evidence.

# 0004: Freezing + construction deprecation plan (Dagster + Iceberg)

- **Status:** Proposed
- **Date:** 2026-05-23

## Purpose
Define a practical, low-bloat plan for identifying and retiring file-centric freezing and construction orchestration elements that are conceptually replaced by Dagster orchestration and Iceberg time-travel/versioning.

This note is intentionally documentation-first and does **not** change statistical/business logic.

## Current state
The current pipeline contains explicit run modes, path management, and CSV lifecycle behaviors for freezing and manual construction workflows.

Representative examples include:
- multiple freezing run switches in user config,
- frozen CSV read/write pathways,
- output files for changes-to-review,
- config validation branches that enforce combinations of freeze/construction file paths,
- helper directory scaffolding for `02_freezing` and `04_construction/manual_construction`.

## Target state
- Dagster assets and asset checks become the control plane for run intent and lineage.
- Iceberg snapshots/tags/metadata become the canonical point-in-time mechanism.
- CSV artefacts remain optional interoperability exports, not canonical run dependencies.

## Migration principles (applied here)
1. Preserve numerical equivalence and business meaning.
2. Keep PRs small and single-purpose.
3. Prefer wrapper/adaptation before removing legacy pathways.
4. Keep each step reversible until reconciliation evidence is stable.

## Inventory and decision buckets
Use three explicit buckets during scan/removal:

### 1) Replace with Dagster/Iceberg capability
Typical traits:
- runtime branching that only selects historical file paths,
- file-system workflows for staged frozen state,
- file naming/version stamping used as a surrogate for table snapshot metadata.

Expected action:
- replace with Dagster run config + Iceberg snapshot/time-travel reads + asset metadata.

### 2) Keep, but relocate execution context
Typical traits:
- reconciliation logic that computes amendments/additions/deletions,
- construction transformations that carry business meaning.

Expected action:
- preserve existing logic; run as assets/checks against Iceberg-backed inputs/outputs.

### 3) Transitional shims
Typical traits:
- user-edited CSV processes still needed operationally.

Expected action:
- keep temporarily behind explicit deprecation boundary while equivalent managed input assets are introduced.

## Proposed phased rollout

### Phase A: audit + contract definition (doc-first)
- Produce an inventory table for freezing/construction elements with fields:
  - `element`, `purpose`, `category` (replace/keep/shim), `replacement`, `owner`, `removal criterion`.
- Publish reduced run-intent contract for Dagster-era execution (e.g. normal, replay, reconciliation).

### Phase B: checks before cuts
- Convert freeze comparison outputs into reconciliation checks/assets.
- For every candidate removal, define equivalence checks:
  - row counts,
  - key-metric aggregates,
  - targeted record-level diff samples.

### Phase C: persistence simplification
- Move canonical frozen-state reads/writes to Iceberg-backed assets.
- Downgrade CSV freeze files to optional export outputs only.

### Phase D: config and scaffolding cleanup
- Remove obsolete mode flags/path validations once equivalent checks are proven.
- Remove mandatory directory scaffolding for deprecated freeze/construction folders.

## Non-goals
- Rewriting statistical methods or business definitions.
- Bulk refactors across unrelated modules.
- Removing manual correction workflows before a safe replacement exists.

## PR template expectations for this stream
Each PR in this stream should include:
- logic-change statement (default: no runtime statistical logic changes),
- assumptions and risks,
- rollback plan,
- tests/checks run,
- equivalence/reconciliation evidence when orchestration or persistence changes.

## Suggested completion criteria per deprecation candidate
A candidate can be removed when all are true:
1. Dagster/Iceberg replacement is live for the same intent.
2. Reconciliation checks pass for agreed run windows.
3. Operational owner confirms no active dependence on legacy path.
4. Rollback path is documented and tested.

# 0004: Legacy deprecation map for lean refoundation

- **Status:** Proposed
- **Date:** 2026-05-23

## Purpose
Provide explicit deprecation intent for legacy concepts so implementation PRs can delete with confidence once parity coverage exists.

## Legend
- **Delete now (doc/runtime references only):** safe to remove immediately if no active dependency.
- **Deprecate later:** keep temporarily while parity oracle is active.
- **Replace with explicit Dagster/Iceberg concept:** remove legacy mechanism and adopt listed replacement.

## Map

### 1) Freezing
- **Current concept:** `src/freezing/*` stage and run modes supporting snapshot comparisons/freeze outputs.
- **Action:** **Deprecate later** then remove after parity checks.
- **Replacement:** **Iceberg snapshot history + time travel + table metadata + release tags**.

### 2) Construction
- **Current concept:** `src/construction/*` hidden mutation/patch stage.
- **Action:** **Deprecate later** then remove after explicit correction assets are in place.
- **Replacement:** **Explicit correction tables/assets** (`ops.response_corrections`, `ops.postcode_corrections`, etc.).

### 3) Runlog
- **Current concept:** `src/utils/runlog.py` run metadata files and log artefacts.
- **Action:** **Replace**.
- **Replacement:** Dagster run records, materialization events, structured logs, asset check results.

### 4) filename/path-amending flow
- **Current concept:** filename mutation/path templating conventions in utility plumbing.
- **Action:** **Deprecate later**.
- **Replacement:** canonical table identifiers + optional export-only naming logic at terminal assets.

### 5) Platform-specific `rd_*` file modules
- **Current concept:** local/network/s3/hdfs injection (`local_file_mods`, `s3_mods`, etc.).
- **Action:** **Deprecate later**.
- **Replacement:** unified Iceberg I/O resources and table readers/writers.

### 6) CSV-first persistence
- **Current concept:** core stages persisting/reading CSV as canonical state.
- **Action:** **Replace**.
- **Replacement:** Iceberg-first storage; CSV as optional export sinks only.

### 7) Legacy run mode flags
- **Current concept:** branchy flags for freeze/construct-only behaviors and compatibility paths.
- **Action:** **Delete now** for new architecture docs and **deprecate later** in runtime until migration cutover.
- **Replacement:** Dagster asset selection/materialization policies and explicit asset config.

## Deletion gates
A legacy item can be removed when all are true:
1. Equivalent business pathway exists in lean Dagster/Iceberg flow.
2. Synthetic fixture + golden/reconciliation checks pass.
3. Rollback path is documented (commit/PR-level).
4. No production dependency remains on removed pathway.

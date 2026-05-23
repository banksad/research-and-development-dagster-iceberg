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


## Concrete inventory seed (from current repository)
The table below is a starting point for Phase A and should be maintained as migration work progresses.

| Element | Current location(s) | Category | Why | Suggested replacement/next step | Owner | Removal criterion |
| --- | --- | --- | --- | --- | --- | --- |
| Multi-mode freezing run switches | `src/user_config.yaml` (`run_with_snapshot`, `run_with_snapshot_and_freeze`, `load_updated_snapshot_for_comparison`, `run_updates_and_freeze`, `run_with_frozen_data`) and `src/utils/config.py` validation | Replace | Encodes orchestration state in file-era booleans; creates combinatorial config complexity. | Replace with a reduced Dagster run-intent contract and partition/snapshot parameters. | Data Platform | New run-intent contract adopted and legacy booleans no longer required in two consecutive monthly runs. |
| Frozen CSV as canonical operational input/output | `src/freezing/freezing_main.py` (`read_frozen_csv`, staged frozen writes) plus `freezing_paths` config in `src/dev_config.yaml`/`src/user_config.yaml` | Replace | Point-in-time persistence represented by files instead of table snapshots. | Read/write canonical frozen-state via Iceberg tables; keep CSV only as optional export. | Data Platform | Iceberg frozen-state assets are canonical and CSV files are confirmed export-only for two consecutive monthly runs. |
| Changes-to-review CSV outputs | `src/freezing/freezing_compare.py` (`output_freezing_files`) | Transitional shim | Useful human review UX but should not be canonical persistence contract. | Materialize review diffs as an asset/check output first; keep CSV export optional. | R&D Methods | Equivalent review asset/check consumed by users, with CSV dependency removed from operational runbook. |
| Manual application of freezing amendments/additions/deletions | `src/freezing/freezing_apply_changes.py` | Transitional shim | User-edited CSV ingestion is operationally useful but file-centric and hard to govern. | Introduce managed correction input assets and migrate apply path to table-aware merge/update semantics. | R&D Methods + Data Platform | Managed correction asset path is live and audited; CSV edit/apply path unused for two consecutive monthly runs. |
| Reconciliation comparison logic (amendments/additions/deletions) | `src/freezing/freezing_compare.py` (`run_comparison`, `get_amendments`, `get_additions_deletions`) | Keep (relocate) | Comparison logic is still needed to prove equivalence/drift. | Retain logic and run as Dagster asset checks over Iceberg-backed inputs. | Data Quality | Checks are materialized in Dagster and pass agreed tolerances for two consecutive monthly runs. |
| Row-level `last_frozen` metadata stamping | `src/freezing/freezing_utils.py` (`_add_last_frozen_column`) | Replace (or compatibility shim) | Snapshot identity can be represented by Iceberg metadata instead of in-row date/run-id strings. | Use Iceberg snapshot/tag metadata as source of truth; keep column only where downstream compatibility requires it. | Data Platform | Downstream consumers confirm no dependency on row-level stamp, or compatibility column maintained via explicit export transform. |
| Construction run toggles and path-coupled validation | `src/user_config.yaml` (`run_all_data_construction`, `run_postcode_construction`, `run_ni_construction`), `src/utils/config.py`, `src/construction/construction_main.py` | Mixed: keep+replace | Some flags are orchestration concerns; construction transforms may encode valid business interventions. | Keep business transforms; move run selection to Dagster configuration and progressively reduce path-coupled switches. | R&D Methods + Data Platform | Construction transforms remain numerically equivalent while orchestration flags are reduced to Dagster run-intent parameters. |
| Directory scaffolding for `02_freezing` and `04_construction` | `helpers/make_network_dirs_main.py`, `helpers/make_s3_dirs_main.py` | Replace | Hard-codes file-era operational layout into bootstrap scripts. | Restrict to optional export dirs only; remove as mandatory runtime prerequisite. | Platform Ops | Production runbooks no longer require creating these folders as preconditions. |

## Initial PR backlog (small, reversible)
1. **PR A1 (doc/catalogue):** add owner + removal criterion columns to this table and agree an acceptance window for deprecations.
2. **PR A2 (checks):** expose freezing comparison outputs as Dagster reconciliation checks (no business-logic rewrite).
3. **PR A3 (persistence):** add Iceberg-backed frozen-state asset read/write path behind feature flag; preserve CSV export path.
4. **PR A4 (config simplification):** add reduced run-intent interface; map legacy flags with explicit deprecation warnings.
5. **PR A5 (cleanup):** remove obsolete freezing/construction file-path validation and scaffolding once checks are green for agreed windows.

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


## Acceptance window for deprecation decisions
To keep removals reversible and evidence-based, each candidate above should meet its removal criterion for **two consecutive monthly production runs** before deletion.

For planning clarity based on this document date (**May 23, 2026**), the earliest default removal window is after successful **June 2026** and **July 2026** runs, with removals targeted from **August 2026** onward unless owners explicitly agree otherwise.

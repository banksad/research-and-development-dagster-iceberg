# 0005: Lite pipeline static audit + removal map

- **Status:** Proposed
- **Date:** 2026-05-23
- **Branch context:** `lite-pipeline`

## Purpose
Provide a concrete, code-referenced removal map for a large-scope "lite pipeline" refactor that simplifies orchestration while preserving core analytical functionality.

This is an **audit-only** note (documentation-first), with no runtime/statistical logic changes.

## Core functionality (keep)
For the lite branch, core runtime flow should still preserve the current analytical pathway:
1. `staging`
2. `mapping`
3. `imputation`
4. (`outlier_detection` + `estimation`) for BERD
5. `site_apportionment`
6. `outputs`

These stages are explicitly wired in `run_pipeline` and remain the baseline functionality to preserve while removing bloat around pre/post pathways. 

## What currently adds orchestration bloat
The main pipeline currently pulls in two legacy-era stages in addition to core flow:
- `run_freezing` is always called after staging.
- `run_construction` can run before mapping and after imputation depending on flags.

In addition, early-return modes exist for freezing-only runs, creating multiple alternate control paths.

## Proposed removal buckets for lite refactor

### Bucket A — Remove immediately (high confidence, orchestration-only)
These are most likely safe to strip in the lite branch when the goal is a single streamlined run mode.

1. **Freezing early-return run modes**
   - `global.load_updated_snapshot_for_comparison`
   - `global.run_updates_and_freeze`
   - Their early returns in `run_pipeline`.

2. **Construction runtime toggles from normal flow**
   - `global.run_all_data_construction`
   - `global.run_postcode_construction`
   - Conditional branches that call `run_construction`.

3. **Path-level validation coupled to removed modes**
   - Freezing-specific validation branches in `validate_freezing_config_settings`.
   - Construction-specific validation branches in `validate_construction_config_settings` where those modes are removed.

4. **Legacy helper scaffolding tied to file-era folder structures**
   - `helpers/make_network_dirs_main.py`
   - `helpers/make_s3_dirs_main.py`
   (Only if no longer required by remaining execution paths.)

### Bucket B — Keep temporarily (business-supporting or needed for confidence)
1. **Core stage transforms and analytical outputs** (staging/mapping/imputation/outlier/estimation/site_apportionment/outputs).
2. **Validation and reconciliation utilities** that can still provide confidence checks during large refactor.
3. **Dagster wrapper placeholders** until replaced by real asset execution paths.

### Bucket C — Defer decision (potentially removable later)
1. NI-specific optional branch (`run_ni`) if scope of lite branch is GB-only (needs explicit business sign-off).
2. Export entrypoints (`export_main.py`, `export_mods_main.py`) after confirming usage.
3. CSV-only compatibility artefacts once Iceberg-backed canonical persistence is in place.

## Suggested execution order for large refactor
1. **Define a single run intent** in config (normal analytical run).
2. **Delete freezing-only control paths** and their config switches.
3. **Delete construction branches** from `run_pipeline` and associated config keys.
4. **Remove dead imports/modules/tests** that become unreachable.
5. **Retain/expand reconciliation checks** against baseline outputs to demonstrate numerical equivalence.

## Evidence sources used for this audit
- `src/pipeline.py` wiring and branch logic for freezing/construction.
- `src/utils/config.py` validation logic for freezing/construction run switches.
- `src/user_config.yaml` global switches currently exposed to users.
- Existing deprecation note in `docs/architecture/0004-freezing-construction-deprecation-plan.md`.

## Assumptions
- "Core functionality" means preserving end analytical outputs for standard BERD/PNP runs.
- This branch accepts large code diffs, but numerical outputs should remain reconcilable.

## Risks and mitigations
- **Risk:** removing construction/freezing paths drops implicitly relied-on corrections.
  - **Mitigation:** capture reconciliation checks before and after each major deletion.
- **Risk:** config simplification breaks legacy runbooks.
  - **Mitigation:** publish a branch-specific migration note and explicit rollback commit points.

## Rollback approach
For each major deletion wave, create a dedicated commit so rollback can be done with commit-level reverts (rather than manual re-edits).

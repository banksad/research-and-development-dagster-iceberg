# AGENTS.md

## Repository mission
This repository is a pandas-based reproducible analytical pipeline for producing business expenditure on research and development statistics.

## Refoundation direction (lean Dagster + Iceberg)
- Target architecture is a **lean Dagster/Iceberg-native analytical pipeline**.
- This project direction is **not** to wrap and preserve the existing file-based pipeline as the end state.
- The existing pipeline may remain temporarily as a **parity oracle** during migration.
- Dagster assets and asset checks are the orchestration layer.
- Apache Iceberg tables are the canonical persistence layer for inputs, intermediates, QA outputs, and final outputs.
- CSV files are optional terminal export artefacts only.

## Hard constraints for all Codex tasks
- Do **not** rewrite statistical or business logic unless explicitly requested.
- Do **not** touch production data.
- Do **not** add secrets, credentials, or real data.
- Prefer synthetic or test fixtures for examples.

## Refoundation migration rules
1. Keep PRs scoped and implementation-sequenced; use documentation-first when direction/contract changes.
2. Prefer extracting core statistical/business transforms into pure pandas/domain functions.
3. Deprecate legacy orchestration and I/O plumbing where it does not contribute to analytical meaning.
4. Do not preserve backwards compatibility for its own sake.
5. Treat freezing/construction as legacy file-era mechanisms, not first-class stages in the target design.
6. Keep changes reversible and explicit, with clear rollback notes.

## Legacy concepts to phase out
- Freezing as a stage: replaced by Iceberg snapshots/history/metadata and explicit release tagging.
- Construction as hidden mutation: replaced by explicit correction input tables/assets (e.g. `ops.response_corrections`, `ops.postcode_corrections`).
- Legacy file-path plumbing and run modes, run logs, platform-specific `rd_*` file modules.

## Numerical equivalence requirements
- Any refactor PR that changes orchestration or persistence must explicitly state expected numerical equivalence with the current pathway.
- For any changed pathway, add reconciliation evidence/checks comparing new-path outputs to oracle-path outputs.

## PR checklist expectations
- Confirm whether runtime Python logic changed (default expectation: no unless requested).
- Document assumptions, risks, and rollback approach.
- Run existing tests/checks relevant to the change.
- Keep changes minimal and scoped to the task.

## Documentation expectations
- Architecture notes should clearly separate:
  - current state,
  - target state,
  - migration principles,
  - non-goals,
  - phased rollout intent,
  - parity/equivalence evidence expectations.

## Dagster wrapper scaffold guardrails
- Do **not** extend `src/orchestration/dagster/definitions.py` as the target architecture.
- New implementation work should target the future lean package, expected at `src/randd_pipeline/`.
- Wrapper-style assets around old stage boundaries are legacy scaffolding, not the desired implementation path.

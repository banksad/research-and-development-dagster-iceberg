# AGENTS.md

## Repository mission
This repository is a pandas-based reproducible analytical pipeline for producing business expenditure on research and development statistics.

## Migration direction (Dagster + Iceberg)
- Long-term orchestration target: Dagster assets and asset checks.
- Long-term persistence target: Apache Iceberg tables as the canonical persistence layer for inputs, intermediate assets, QA outputs, and final outputs.
- CSV files should be treated as optional export artefacts, not canonical outputs.

## Hard constraints for all Codex tasks
- Do **not** rewrite statistical or business logic unless explicitly requested.
- Do **not** touch production data.
- Do **not** add secrets, credentials, or real data.
- Prefer synthetic or test fixtures for examples.

## Safe migration rules
1. Keep PRs small, single-purpose, and easy to review on mobile.
2. Prefer documentation-first changes before implementation refactors.
3. When introducing Dagster, initially wrap existing `run_*` functions as assets before deeper refactors.
4. Preserve stage boundaries at first to reduce regression risk.
5. Reuse existing config and schema validation logic for Dagster asset checks where possible.
6. Keep migration steps reversible and explicit.

## Branch-specific working mode (`lite-pipeline`)
- On the `lite-pipeline` branch, larger PRs are explicitly allowed, including significant multi-file changes, to support the planned large-scale simplification refactor.
- For this branch, prioritise coherent refactor batches over mobile-sized PRs, while still documenting assumptions, risks, rollback, and validation evidence.

## Numerical equivalence requirements
- Any migration PR that changes orchestration or persistence must explicitly state expected numerical equivalence with the current pipeline.
- For any changed pathway, add reconciliation evidence or checks comparing new-path outputs to current-path outputs.

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
  - and phased rollout intent.

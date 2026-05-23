# Dagster bootstrap scaffold (PR-1)

## Current state
The production pipeline is orchestrated by `src/pipeline.py` and remains the default execution path.

## Target state
Dagster assets and asset checks orchestrate the pipeline, with Apache Iceberg as canonical persistence for inputs, intermediates, QA outputs, and final outputs.

## Migration principles (this PR)
- Additive orchestration scaffolding only.
- Preserve current stage boundaries one-to-one.
- Keep changes small, reversible, and easy to review.

## Non-goals (this PR)
- No statistical/business logic changes.
- No canonical persistence change.
- No default-runner switch from `src/pipeline.py`.

## Rollback
Remove `src/orchestration/dagster/` and any references to it. The existing `src/pipeline.py` path remains intact.

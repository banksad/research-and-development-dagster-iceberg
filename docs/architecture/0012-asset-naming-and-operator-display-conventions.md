# ADR 0012: Asset naming and operator display conventions

- **Status:** Proposed
- **Date:** 2026-05-23

## 1. Purpose

In the lean Dagster + Iceberg refoundation pipeline, production-facing Dagster asset names, asset check names, descriptions, metadata, and failure/warning messages are part of the production operator interface.

Production operators are not expected to read Python implementation details in order to run, inspect, and triage the pipeline. The Dagster UI and Launchpad should communicate stage intent and outcomes in business/statistical terms that are readable by non-programmer operators.

## 2. Audience

Primary audiences for these conventions are:

- **Production operators**: configure and launch runs; inspect DAG state, materialisations, warnings, and failures; and identify likely operational actions.
- **Statisticians / methodologists**: validate that stage-level outputs, QA checks, and messaging align with analytical intent.
- **Data scientists / data engineers**: implement and maintain assets/checks while preserving operator-readable display semantics.
- **Cloud/platform engineer**: support deployment/runtime controls and ensure production launch/config experience remains operable and understandable.

## 3. Production-facing asset naming principles

For the main production DAG view:

1. Use **stage-level names** for production-facing assets.
2. Prefer **business/statistical terminology** over implementation/mechanical detail.
3. Avoid exposing temporary seam names as primary production stage names.
4. Keep names stable once downstream stages depend on them.
5. Use clear namespace prefixes:
   - `raw/`
   - `ref/`
   - `ops/`
   - `intermediate/`
   - `qa/`
   - `mart/`
6. Avoid abbreviations unless they are widely understood by the production team.
7. Avoid legacy names that only make sense in the old file-era pipeline.

## 4. Recommended production-facing stage assets

Intended high-level production asset names include:

- `raw/full_responses`
- `ref/...` reference inputs
- `ops/...` operational correction/adjustment inputs
- `intermediate/staged_responses`
- `intermediate/mapped_responses`
- `intermediate/imputed_responses`
- `intermediate/outlier_adjusted_responses`
- `intermediate/estimated_responses`
- `intermediate/site_apportioned_responses`
- `mart/final_outputs`

These represent operator-facing stage intent. Exact names may evolve during implementation, but production-facing names should remain intuitive and stage-level.

## 5. Development/debug/checkpoint asset naming

During refoundation, temporary seam-level assets may be introduced for checkpointing, debugging, parity comparison, or migration risk control.

For example, `intermediate/cell_number_mapped_responses` may exist as a temporary checkpoint/debug asset.

Conventions:

- Temporary seam assets should not automatically become permanent production-facing stages.
- If a seam asset is retained beyond temporary migration use, documentation should explicitly mark it as `debug/checkpoint` (or equivalent wording).
- Any future seam-specific table retained long-term should be explicitly justified in architecture documentation.

## 6. Check naming principles

Asset checks should be named for operator readability and triage value.

Examples of useful check names:

- `required_columns`
- `unique_response_grain`
- `reference_mapper_coverage`
- `cell_number_mapping_complete`
- `no_unexpected_unmapped_records`
- `published_totals_reconcile`

Examples of less useful check names:

- `validate_df_2`
- `join_check`
- `mapping_columns_present`
- `test_cell_no`
- names copied directly from legacy function internals

Check failure/warning messages should:

- state what failed;
- state why it matters;
- identify the affected stage/table;
- provide enough detail for operator/statistician triage;
- avoid relying on raw Python tracebacks as the primary explanation where possible.

## 7. Asset metadata principles

Where useful, production-facing assets should eventually expose clear operational metadata in Dagster, such as:

- table identifier;
- row count;
- column count;
- survey year / period;
- source table snapshot IDs where available;
- reference/correction snapshot IDs where available;
- run purpose;
- warning counts;
- QA table links or identifiers.

This ADR defines display principles only; it does **not** implement metadata emission in runtime code.

## 8. Run config display principles

Production Launchpad/run config display should:

- use clear field names;
- minimise raw file-path style inputs in production contexts;
- expose survey year / period / run purpose clearly;
- use constrained options where possible;
- avoid secrets in operator-facing config;
- validate early with readable messages.

## 9. Examples (before/after)

Before (less operator-friendly production display):

- asset: `cell_number_mapped_responses`
- check: `mapping_columns_present`
- function: `run_mapping`

After (operator-friendly production display while retaining technical internal naming freedom):

- production asset: `intermediate/mapped_responses`
- check: `cell_number_mapping_complete`
- internal domain function: `apply_cell_number_mapping(...)`

Internal function/module names may remain technical where appropriate. Production-facing asset/check names shown in Dagster should remain stage-level and operator-friendly.

## 10. Relationship to existing ADRs

This ADR:

- supports `0010-production-operating-model.md` by improving production usability in Dagster UI/Launchpad;
- supports `0011-mapping-consolidation-plan.md` by separating stage-level operator naming from temporary migration seams;
- reinforces `0009-mapping-table-shape.md` by keeping hybrid mapping implementation choices compatible with clear production-facing stage semantics.

## 11. Acceptance criteria

- New ADR exists at `docs/architecture/0012-asset-naming-and-operator-display-conventions.md`.
- It defines production-facing naming principles.
- It distinguishes production assets from development/debug/checkpoint assets.
- It defines check naming and message principles.
- It explains why asset names are part of the operator UI.
- No runtime code is changed.
- No tests are changed.
- No table contracts are changed.
- PR is opened as a draft against `develop`.

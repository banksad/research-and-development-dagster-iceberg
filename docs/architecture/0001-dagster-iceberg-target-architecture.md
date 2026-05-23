# 0001: Dagster + Iceberg target architecture

- **Status:** Proposed
- **Date:** 2026-05-23

## Context
This project currently runs as a pandas-based analytical pipeline with stage-oriented `run_*` functions orchestrated by the main pipeline entrypoint. The objective is to evolve orchestration and persistence without changing statistical intent or business meaning.

## Decision
Adopt a target architecture where:

1. **Dagster assets** become the primary orchestration abstraction.
2. **Apache Iceberg tables** become the canonical persistence layer for:
   - source inputs,
   - intermediate assets,
   - QA outputs,
   - final analytical outputs.
3. **CSV files** become optional export artefacts for interoperability and downstream consumers that still require file-based extracts.

## Initial migration shape
To reduce risk, initial Dagster assets should wrap existing `run_*` functions rather than rewriting internals. This preserves known stage boundaries and behavior while introducing orchestration metadata, lineage, and asset-level observability.

## Dagster asset checks
Asset checks should be introduced incrementally and grouped into:
- configuration validation,
- schema validation,
- data quality checks,
- reconciliation checks.

Where possible, these checks should reuse existing configuration/schema validation logic to avoid duplicate validation rules.

## Numerical equivalence expectation
Migration work must preserve numerical outputs and interpretations relative to the current pipeline. Any deviation must be explicit, justified, and approved.

## Consequences
### Positive
- Clear lineage and orchestration visibility through asset materializations.
- Canonical table-based persistence suitable for reproducibility and governed evolution.
- Reduced coupling to CSV as a storage mechanism.

### Trade-offs
- Temporary dual representations (Iceberg canonical + CSV exports) may increase complexity during transition.
- Team will need operational conventions for asset/check ownership and release discipline.

## Non-goals (for early PRs)
- Rewriting statistical algorithms.
- Changing business definitions/metrics.
- Migrating all stages in a single PR.

## Phased rollout intent
- **PR 0:** documentation and migration guardrails.
- **PR 1+:** thin Dagster scaffolding and pilot wrapper assets.
- **Later phases:** broaden asset coverage, introduce canonical Iceberg writes, and progressively move CSV to export-only responsibilities.

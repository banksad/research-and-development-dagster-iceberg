# 0003: Refoundation strategy — lean Dagster/Iceberg pipeline

- **Status:** Proposed
- **Date:** 2026-05-23
- **Supersedes direction of:** conservative wrapper-first migration notes

## Context
The previous migration posture focused on wrapping the existing file-based runtime in Dagster/Iceberg scaffolding. This preserved too much legacy orchestration, platform/file plumbing, and run-mode complexity.

## Decision
Adopt a **lean refoundation** strategy:

1. Build a Dagster-native analytical pipeline whose core is business/statistical transforms.
2. Use Apache Iceberg tables as canonical persistence for inputs, intermediates, QA, and final outputs.
3. Treat CSV files as optional terminal export artefacts only.
4. Do not carry forward freezing and construction as first-class pipeline stages.
5. Keep old pipeline temporarily only as a parity oracle during phased migration.

## What this is not
- Not a long-term wrapper around `src/pipeline.py`.
- Not a compatibility exercise to preserve legacy run modes and path conventions.
- Not a requirement to keep platform-specific `rd_*` file module injection patterns.

## Principles
- Preserve validated statistical/business logic unless explicitly changed by business decision.
- Separate business logic from orchestration, I/O, config/path plumbing, and export formatting.
- Prefer pure pandas transformation functions that are testable in isolation.
- Model amendments/corrections as explicit inputs/assets, not hidden mutation steps.

## Freezing and construction position
- **Freezing:** deprecate as a stage. Replace with Iceberg snapshot history, table metadata, time travel, and explicit release tagging.
- **Construction:** deprecate as hidden mutation stage. Replace with explicit correction inputs (e.g., `ops.response_corrections`, `ops.postcode_corrections`) with auditable lineage.

## Data and security constraints
- Do not use real data, private data, secrets, credentials, or production paths in implementation/testing artefacts.
- Use synthetic fixtures and golden tests to prove parity/equivalence.

## Implementation posture
- Sequence into small, reviewable PRs.
- Keep old and new pathways side-by-side only as long as needed to establish parity.
- Remove/deprecate legacy components once equivalent behavior is covered by tests/checks.

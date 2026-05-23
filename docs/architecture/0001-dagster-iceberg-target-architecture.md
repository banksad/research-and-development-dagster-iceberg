# 0001: Dagster + Iceberg target architecture

- **Status:** Proposed
- **Date:** 2026-05-23

## Current state
- The repository currently operates as a pandas-based analytical pipeline with substantial legacy file-path and run-mode orchestration.
- Legacy concepts such as freezing/construction exist as file-era workflow mechanisms.
- Existing pathway remains useful as a **parity oracle** during migration, but is not the desired end-state runtime.

## Target state
Adopt a **lean Dagster/Iceberg-native analytical pipeline** where:

1. **Dagster assets + asset checks** are the orchestration and quality-control layer.
2. **Apache Iceberg tables** are the canonical persistence layer for:
   - source inputs,
   - intermediate assets,
   - QA outputs,
   - final analytical outputs.
3. **CSV files** are optional terminal export artefacts only.
4. Legacy freezing/construction behavior is replaced by explicit, auditable data-model mechanisms.

## Migration principles alignment
This architecture is implemented under the following constraints:
- Preserve statistical/business meaning unless explicitly approved to change.
- Keep migration PRs scoped, reversible, and sequenced.
- Prefer extraction of core transforms into pure pandas/domain functions.
- Deprecate legacy orchestration/plumbing that does not contribute analytical meaning.
- Treat old pipeline as temporary parity support only.

## Parity and equivalence expectations
Any orchestration/persistence pathway change must:
- declare expected numerical equivalence scope,
- add reconciliation checks/evidence against the oracle pathway,
- classify and explain any observed differences.

## Non-goals
- Preserving legacy runtime compatibility for its own sake.
- Reintroducing file-path plumbing as a long-term contract.
- Rewriting validated statistical logic without explicit request.

## Phased rollout intent
- **Phase 0 (docs/contracts):** architecture notes, principles, deprecation mapping, parity expectations.
- **Phase 1 (logic boundary):** isolate business transforms from legacy orchestration and file I/O.
- **Phase 2 (Dagster/Iceberg implementation):** materialize canonical inputs/intermediates/QA/finals as assets/tables.
- **Phase 3 (deprecation):** retire legacy freezing/construction/file-era modules once parity is demonstrated.

# 0002: Migration principles for Dagster + Iceberg adoption

- **Status:** Proposed
- **Date:** 2026-05-23

## Purpose
Define the safe, repeatable principles that guide migration from the current pandas/file-oriented pipeline to Dagster-orchestrated, Iceberg-canonical data assets.

## Core principles
1. **Numerical equivalence first**
   - Preserve analytical outputs and interpretation unless a change is explicitly approved.
2. **Wrap before rewrite**
   - Dagster assets should initially call existing `run_*` functions.
3. **Canonical persistence clarity**
   - Iceberg tables are canonical; CSV files are export artefacts.
4. **Incremental and reversible delivery**
   - Keep PRs small, isolate one migration axis at a time, and maintain rollback paths.
5. **Validation reuse over duplication**
   - Reuse existing config/schema validation logic in Dagster asset checks where practical.

## Equivalence and reconciliation policy
For any migration PR that introduces new orchestration or persistence behavior:
- state expected equivalence scope,
- define comparison granularity (table-level and key-metric-level where relevant),
- provide reconciliation outputs/checks,
- classify any differences as expected or defects.

## Safe change sequencing
Recommended order for implementation PRs after this document set:
1. Dagster project scaffolding (no logic changes).
2. Wrapper assets around existing stage functions.
3. Asset checks mapped to existing validation logic.
4. Controlled Iceberg materialization for selected assets.
5. Broader persistence transition and CSV export decoupling.

## Guardrails for future PRs
Each migration PR should explicitly include:
- scope and non-goals,
- logic-change statement,
- risk assessment and rollback notes,
- tests/checks run,
- evidence of equivalence/reconciliation when applicable.

## Risks to monitor
- Hidden behavioral drift when changing orchestration boundaries.
- Conflicting definitions of “equivalence” across contributors.
- Temporary operational overhead during dual-output transition.

## Assumptions
- Existing run-stage functions are reliable baselines for wrapper-asset adoption.
- Existing test coverage provides a minimum safety net for no-logic-change migration steps.
- Teams accept temporary coexistence of canonical Iceberg assets and CSV export artefacts during transition.

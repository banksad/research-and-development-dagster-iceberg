# ADR 0014: Dagster-native feature alignment for lean refoundation

- **Status:** Proposed
- **Date:** 2026-05-24

## 1. Context

The refoundation now has first-class Dagster assets, asset checks, resources, and an operator-facing Dagster `Config` class for minimal imputation settings.

Before adding further business logic, we need an explicit decision record for which Dagster-native capabilities should be used directly, and where custom framework code is intentionally out of scope.

## 2. Decision

For the lean Dagster/Iceberg target architecture, default to Dagster-native features first, only adding custom abstractions where there is a clear analytical/business requirement not met by Dagster.

### 2.1 Dagster-native features to use by default

- **Assets** for stage contracts and lineage (e.g., `raw/*`, `intermediate/*`, `outputs/*`).
- **Asset checks** for data-quality and contract validation instead of bespoke check runners.
- **Resources** for runtime integration boundaries (e.g., table-store access and related adapters).
- **Dagster Config classes** for operator-facing run-time parameters exposed in Launchpad/job run config.
- **Dagster run metadata and event log** for run observability rather than custom run-log files.
- **Dagster selection/backfills/re-execution** for controlled partial reruns.

### 2.2 Explicit non-goals

- Building a bespoke orchestration mini-framework on top of Dagster.
- Recreating file-era run-mode/path-plumbing behaviour in new runtime code.
- Embedding statistical/business logic in orchestration wrappers when it belongs in pure domain functions.

## 3. Current state vs target state

### Current state

- Minimal mapping and imputation seams are implemented with Dagster assets and checks.
- First operator-facing imputation `Config` exists for simple TMI-style behaviour.
- Legacy pipeline remains available as parity oracle for migration evidence.

### Target state

- All new refoundation stages are expressed as Dagster assets with explicit table contracts.
- Quality gates are enforced through asset checks and check metadata/QA tables.
- Operator controls are passed through Dagster Config and Launchpad/job run config, not custom YAML/path glue.

## 4. Migration principles reinforced

- Keep core analytical logic in `src/randd_pipeline/domain/*` as pure pandas/domain transforms.
- Keep orchestration in Dagster assets/resources/checks without introducing new framework layers.
- Preserve explicit parity/equivalence evidence when orchestration or persistence changes.
- Keep changes scoped and reversible.

## 5. Consequences

### Positive

- Reduces risk of accidental custom-framework growth.
- Improves operability by aligning config and run controls with Dagster UI/Launchpad.
- Keeps migration intent consistent with lean target architecture ADRs.

### Trade-offs

- Some legacy convenience behaviours (file-era path/run-mode toggles) are intentionally not carried forward.
- Additional up-front contract/config documentation is required when introducing new seams.

## 6. Rollout intent

- Apply this ADR as a design guardrail for upcoming imputation and downstream seam PRs.
- Require each relevant PR to state whether it uses Dagster-native primitives or introduces any custom abstraction (and why).
- Keep the legacy route as oracle-only until seam-level parity evidence is complete.

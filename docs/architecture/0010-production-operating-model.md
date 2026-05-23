# 0010: Production operating model for lean Dagster/Iceberg pipeline

- **Status:** Proposed
- **Date:** 2026-05-23

## Purpose
This ADR defines how the refoundation pipeline should be operated in production using Dagster and Iceberg.

The operating model is explicitly designed for non-programmer production operation, with Dagster as the operational control plane and Apache Iceberg as the reproducible table/versioning layer. Asset checks are a central product feature for fault prevention, data-quality visibility, and statistical assurance, not an afterthought.

The objective of the refoundation is not only code modernisation: it is to reduce faults, silent failures, and unseen data-quality problems while making statistical production reproducible and auditable.

## Production user model

### 1) Production operators
**Need to see**
- A readable stage-level DAG and run history in Dagster.
- Asset materialisations, check outcomes, warnings, and failures.
- Clear run purpose/context (e.g., survey year/period).

**Should be able to do**
- Launch and monitor runs from Dagster Launchpad and/or configured jobs.
- Inspect run status, logs, check messages, and asset lineage.
- Apply controlled record-level corrections through explicit correction input assets/tables.

**Should not need to do**
- Edit Python code to perform standard production operations.
- Depend on hidden mutation-style steps.
- Manually orchestrate file-path-based workflows.

### 2) Statisticians / methodologists
**Need to see**
- Evidence that transformations and outputs are statistically correct.
- Asset checks and diagnostics in business/statistical language.
- Reproducibility evidence for published outputs.

**Should be able to do**
- Review quality checks, reconciliation signals, and run-to-run diagnostics.
- Trace outputs back to inputs, references, and correction states.

**Should not need to do**
- Reverse-engineer low-level orchestration internals to understand data quality.
- Rely on implicit/undocumented manual interventions.

### 3) Data scientists / data engineers
**Need to see**
- Clear contracts for stage assets, checks, and correction inputs.
- Operational expectations for reproducibility and failure behavior.

**Should be able to do**
- Maintain and extend pipeline logic safely.
- Improve checks and diagnostics with explicit contracts.
- Investigate failures with run metadata and table snapshots.

**Should not need to do**
- Preserve legacy orchestration patterns that no longer add analytical value.
- Carry hidden operational state outside assets/tables.

### 4) Cloud engineer / platform owner
**Need to see**
- Clear separation between local v1 operating model and later cloud deployment concerns.
- Explicit future requirements for IAM, scheduling, runtime images, and storage.

**Should be able to do**
- Provision and maintain eventual GCP infrastructure aligned to this operating model.
- Implement platform controls without changing statistical intent.

**Should not need to do**
- Infer production behavior from ad hoc scripts or undocumented run conventions.
- Block local model validation while waiting for full cloud implementation.

## Production-facing Dagster principles
The production DAG should be stage-level, readable, and operationally meaningful.

Principles:
- Prefer one canonical table per major analytical stage where possible.
- Keep reference/correction inputs explicit as first-class upstream assets.
- Attach checks to assets using business/statistical language.
- Make warning/failure signals visible in the Dagster UI.
- Avoid permanent graph sprawl from every internal seam unless justified.
- Do not return to legacy `run_mapping`, freezing, construction, or file-path orchestration.

Suggested conceptual DAG:

```text
raw/full_responses
  -> intermediate/staged_responses
  -> intermediate/mapped_responses
  -> intermediate/imputed_responses
  -> intermediate/outlier_adjusted_responses
  -> intermediate/estimated_responses
  -> intermediate/site_apportioned_responses
  -> mart/final_outputs
```

Reference and operational inputs should feed relevant stages, for example:

```text
ref/ultfoc_mapper
ref/cell_number_mapper
ops/response_corrections
ops/manual_outliers
ops/manual_trimming
  -> relevant intermediate assets
```

## Run configuration principles
Dagster run configuration for production operators should be:
- Typed, validated, and minimal.
- Expressed with clear field names and descriptions.
- Constrained to known-safe choices where possible.
- Explicit about survey year, period, and run purpose.
- Free from secrets and credentials.
- Free from raw file paths in long-term production operation unless explicitly justified.
- Validated early with readable error messages suitable for non-programmer operators.

Local synthetic demo configuration may be simpler than eventual GCP/lakehouse production configuration, but should preserve the same operational intent and validation philosophy.

## Check and failure policy (initial version)

### Blocking failures (must fail/stop relevant asset or run)
- Missing required columns.
- Duplicate primary analytical grain.
- Invalid reference mapper keys.
- Unmatched mandatory mappings.
- Impossible or invalid values.
- Failed reconciliation checks that make output unsafe.

### Warnings / review items (visible, non-blocking by default)
- Unusual but possible values.
- Large changes versus prior run.
- Optional mapper gaps where defaulting is allowed.
- Diagnostics requiring human review.

Policy statements:
- Blocking checks should fail the relevant asset or run.
- Warning checks should be clearly visible to production operators and statisticians.
- Check messages must be understandable without reading code.
- Useful diagnostics should eventually be materialised into QA tables/assets for review and audit.

## Record-level corrections / adjustments model
Production users may need to make record-level changes identified during operation. These changes must be explicit, auditable, versioned inputs and not hidden mutations.

Initial conceptual operational input tables/assets (not implemented by this ADR):
- `ops.response_corrections`
- `ops.reference_overrides`
- `ops.manual_outliers`
- `ops.manual_trimming`
- `ops.publication_adjustments` (if needed later)

Recommended correction fields per record:
- target table/asset
- affected key (e.g., reference/instance/survey year/question code)
- old value (if known)
- new value
- reason
- requester
- approver
- timestamp
- effective run or period
- status
- lineage reference such as `correction_id` and/or `superseded_by`

Direction:
- Corrections should be applied by explicit assets in the DAG.
- Correction application should itself have checks.
- Iceberg snapshots should preserve before/after states to support audit and rollback analysis.

## Reproducibility contract
For any published/statistical run, the following should be recoverable:
- code commit SHA
- container image tag/digest (eventual)
- Dagster run id
- run config snapshot
- input Iceberg table snapshots
- reference table snapshots
- correction/ops table snapshots
- output table snapshots
- asset check results
- relevant warnings/errors
- release/publication tag or label (if used)
- final output table version

The target system should support:
- Querying Iceberg tables as-of a snapshot or timestamp.
- Identifying which input/config/correction state produced a given output.
- Reproducing or investigating a published statistic from recorded metadata and snapshots.

This ADR defines the target contract and does not implement it.

## Local v1 vs later GCP deployment
Local v1 should demonstrate the operating model with synthetic data by showing:
- stage-level DAG behavior,
- checks and failure-mode visibility,
- correction-model direction,
- reproducibility metadata expectations conceptually.

Later work (not blocked by local v1) includes:
- GCP deployment,
- Artifact Registry,
- lakehouse microservice integration,
- IAM hardening,
- production scheduling and operations.

Do not block local v1 validation on complete cloud deployment implementation.

## Benefits to demonstrate
The refoundation pipeline should demonstrate:
- fewer silent failures,
- visible and understandable asset checks,
- clearer production operation for non-programmer operators,
- auditable record-level corrections,
- reproducible statistics through Iceberg snapshots and Dagster metadata,
- reduced reliance on hidden files and expert memory,
- safer change management,
- clearer handoff between production operators, statisticians, data scientists, and cloud engineers.

## Open questions
Future ADRs/PRs should resolve:
- exact correction table schemas,
- check severity model details and warning representation in Dagster (native or separate QA assets),
- release-tagging strategy for Iceberg snapshots,
- how operators enter corrections in practice,
- whether a lightweight UI/form is needed outside Dagster,
- correction approval workflow/authority,
- required historical retention for data/snapshots,
- how production operators query Iceberg snapshots,
- what metadata belongs in release manifests.

## Acceptance criteria
- [x] New ADR exists at `docs/architecture/0010-production-operating-model.md`.
- [x] Describes production operators as non-programmer Dagster users.
- [x] Explains Dagster UI/Launchpad, asset checks, and run history for operation.
- [x] Explains explicit correction inputs replacing hidden mutation/construction behavior.
- [x] Defines a reproducibility contract involving Dagster metadata and Iceberg snapshots.
- [x] Distinguishes local synthetic v1 from later GCP deployment.
- [x] Explains benefits in quality, fault reduction, reproducibility, and operational clarity.
- [x] Runtime code unchanged.
- [x] Tests unchanged.
- [x] Table contracts unchanged.

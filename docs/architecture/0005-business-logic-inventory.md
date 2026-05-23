# 0005: Business-logic inventory for lean refoundation

- **Status:** Proposed
- **Date:** 2026-05-23
- **Scope:** `src/` static inventory to drive follow-on implementation PRs

## Classification key
1. Keep mostly intact
2. Extract business logic from
3. Rewrite
4. Deprecate
5. Delete after parity coverage exists
6. Unclear / needs human decision

## Inventory table

| Path/module | Current role | Classification | Reason | Proposed future location | Suggested test coverage | Type |
|---|---|---:|---|---|---|---|
| `src/staging/*` | Ingest parsing/validation/transmutation for survey inputs | 2 | Contains valuable transformations + validation but entangled with stage runner/file plumbing | `src/randd_pipeline/domain/staging/*` + `assets/staging.py` + `checks/schema_checks.py` | Unit tests on pure transforms + schema checks + golden staged fixture parity | Business + validation |
| `src/mapping/*` | Postcode/PG/SIC/ITL and related mapping logic | 2 | Core mapping logic should be retained; stage wrappers and I/O coupling removed | `domain/mapping/*`, `assets/mapping.py`, `checks/reconciliation_checks.py` | Unit tests for each mapping + key integrity checks + golden parity | Business |
| `src/imputation/*` | Core imputation transformations (MoR, TMI, SF/LF, helpers) | 2 | High-value statistical logic with mixed orchestration concerns | `domain/imputation/*`, `assets/imputation.py` | Existing unit tests ported + golden end-to-end imputation parity | Business |
| `src/outlier_detection/*` | Manual/auto outlier handling | 2 | Analytical logic should survive, runtime shell should be modernized | `domain/outliers/*`, `assets/outliers.py` | Unit tests + BERD branch parity fixtures | Business |
| `src/estimation/*` | Weighting and estimation computations | 2 | Core statistical function set to preserve | `domain/estimation/*`, `assets/estimation.py` | Unit tests + aggregate reconciliation checks | Business |
| `src/site_apportionment/*` | Apportionment calculations and status filtering | 2 | Key downstream business transform; likely mixed with runner concerns | `domain/apportionment/*`, `assets/apportionment.py` | Unit tests + golden apportionment dataset parity | Business |
| `src/outputs/*` | Final analytical outputs + CSV/file exports + formatting | 2 | Separate analytical output derivation from file export and format concerns | `domain/outputs/*`, `assets/outputs.py`, `assets/exports.py` | Unit tests for output metrics + optional export smoke tests | Business + output formatting + I/O |
| `src/freezing/*` | Snapshot compare/freeze stage behavior | 4 | File-era state management superseded by Iceberg snapshot semantics | N/A (replaced by Iceberg features + checks) | Transitional regression only until removal | Orchestration + I/O |
| `src/construction/*` | Ad hoc data mutation/construction stage | 4 | Hidden mutation not acceptable in target design; replace with explicit corrections inputs | `assets/inputs.py` (`ops.*` corrections tables) | Correction-asset tests + parity checks where legacy used | Business-adjacent mutation + I/O |
| `src/pipeline.py` | Monolithic runtime orchestrator with run modes/platform injection | 5 | Conflicts with Dagster-native orchestration target | `definitions.py` + asset graph | Integration parity test during migration, then retire | Orchestration |
| `src/orchestration/dagster/definitions.py` | Placeholder wrapper asset list | 3 | Must become real lean asset graph, not wrapper placeholders | `src/randd_pipeline/definitions.py` | Import tests + asset dependency graph tests | Orchestration |
| `src/utils/runlog.py` | File-based run logging | 5 | Superseded by Dagster metadata/events | N/A (Dagster metadata) | None after replacement; temporary smoke check only | Orchestration/I/O |
| `src/utils/path_helpers.py` | Path/filename templating and validation plumbing | 4 | Large parts are legacy file-path coupling | Split: minimal reusable validation into `checks/` if still relevant | Unit tests only for retained validation helpers | Config/path plumbing |
| `src/utils/local_file_mods.py` | Local/network file adapters (`rd_*`) | 5 | Platform-specific adapter model not needed for Iceberg-first assets | `io/iceberg.py` resource layer | I/O integration tests against local test catalog | I/O |
| `src/utils/s3_mods.py` | S3 file adapter layer | 5 | Same as above; file adapter model deprecated | `io/iceberg.py` (warehouse/catalog config) | I/O integration tests in non-prod env | I/O |
| `src/utils/singleton_boto.py` | Client singleton for legacy S3 plumbing | 5 | Legacy runtime concern not part of lean domain pipeline | N/A | None after removal | I/O/plumbing |
| `src/utils/config.py` | YAML merge/validation + legacy run mode checks | 3 | Keep validation ideas; remove legacy run-mode/platform assumptions | `randd_pipeline/config/models.py` + `checks/config_checks.py` | Unit tests for config model + invariants | Config |
| `src/utils/breakdown_validation.py` | Output breakdown validations | 1 | Likely reusable validation logic with minimal changes | `checks/dq_checks.py` | Existing tests + asset check integration tests | Validation |
| `src/northern_ireland/*` | NI-specific staging/conversion branch | 6 | Need business decision on whether NI path remains in lean scope | `domain/ni/*` and dedicated assets if retained | Branch-specific parity tests | Business + orchestration |
| `src/pipeline.py` freeze/construct flags usage | Early-return and branch run modes | 4 | Legacy control flow to be retired | Dagster selection/config policies | Migration-time only parity tests | Orchestration |

## Explicit focus items

### `src/freezing/*`
- **Role:** legacy snapshot/freeze handling for CSV/file persistence.
- **Plan:** deprecate and remove after parity evidence; replace with Iceberg snapshot primitives and release metadata.

### `src/construction/*`
- **Role:** hidden data mutation path.
- **Plan:** deprecate and replace with explicit correction assets/tables.

### `src/pipeline.py`
- **Role:** single-entry orchestration mixing config/path/I/O and business stage wiring.
- **Plan:** retire after lean Dagster definitions own orchestration.

### `src/utils/runlog.py`
- **Role:** runtime log files and run-id plumbing.
- **Plan:** remove in favor of Dagster-native run/asset metadata.

### `src/utils/path_helpers.py`, `src/utils/local_file_mods.py`, `src/utils/s3_mods.py`
- **Role:** file-path and storage backend plumbing.
- **Plan:** deprecate/delete as Iceberg-first resource layer replaces platform-specific file adapters.

### Output modules (`src/outputs/*`)
- **Role:** mixed analytics + formatting + file export.
- **Plan:** keep analytical generation logic; separate optional export side effects.

### Staging/Mapping/Imputation/Outlier/Estimation/Site apportionment
- **Role:** core analytical transforms.
- **Plan:** preserve validated formula logic; extract into pure pandas functions and wire through assets.

## Constraints carried into implementation
- No real data, secrets, credentials, production paths, or private datasets.
- Use synthetic fixtures and golden tests.
- Do not rewrite statistical formulas unless explicitly approved.

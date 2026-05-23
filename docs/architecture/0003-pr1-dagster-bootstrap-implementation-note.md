# 0003 — PR-1 implementation note: Dagster bootstrap stage mapping

## Purpose
Traceability note for the additive Dagster bootstrap layer introduced in PR-1.

## Runtime impact
- Runtime Python business/statistical logic: **unchanged**.
- Default orchestration entrypoint: **unchanged** (`src/pipeline.py`).
- Persistence model: **unchanged** (no Iceberg canonical switch in this PR).

## Asset-to-stage mapping
| Dagster asset name | Existing pipeline stage function |
|---|---|
| `staging` | `src.staging.staging_main.run_staging` |
| `freezing` | `src.freezing.freezing_main.run_freezing` |
| `ni` | `src.northern_ireland.ni_main.run_ni` |
| `construction` | `src.construction.construction_main.run_construction` |
| `mapping` | `src.mapping.mapping_main.run_mapping` |
| `imputation` | `src.imputation.imputation_main.run_imputation` |
| `outlier` | `src.outlier_detection.outlier_main.run_outliers` |
| `estimation` | `src.estimation.estimation_main.run_estimation` |
| `site_apportionment` | `src.site_apportionment.site_apportionment_main.run_site_apportionment` |
| `outputs` | `src.outputs.outputs_main.run_outputs` |

## Assumptions and risks
- Assumption: stage-level config and IO callables will be injected in a later PR.
- Risk: placeholder wrappers could drift from `run_*` signatures if not updated during refactors.

## Numerical equivalence expectation
No output changes expected because this orchestration path is additive and non-default.

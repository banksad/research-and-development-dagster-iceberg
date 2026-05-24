# Local synthetic v1 readiness checklist

## 1) Scope

- [ ] This declaration covers local synthetic v1 only.
- [ ] This declaration excludes GCP infrastructure/configuration.
- [ ] This declaration is not a production deployment sign-off.
- [ ] This declaration does not claim full legacy parity.
- [ ] CSV/Excel/API dissemination is not included yet.

## 2) What works

- [ ] Full synthetic chain materialises end-to-end.
- [ ] Major input/reference/intermediate/curated tables are produced.
- [ ] Asset checks are registered and exercised.
- [ ] Final canonical output is `curated.rnd_statistics`.
- [ ] Downstream dissemination strategy (table-first, exports later) is documented.

## 3) What is intentionally deferred

- [ ] Production raw ingestion.
- [ ] Production Iceberg/catalog deployment.
- [ ] GCP infrastructure.
- [ ] Snapshot/release tagging.
- [ ] API serving.
- [ ] CSV/Excel exports.
- [ ] Full legacy output parity.
- [ ] Performance testing.
- [ ] Operator training.

## 4) Acceptance criteria for declaring local synthetic v1

- [ ] `pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py` passes in an environment with pandas + dagster.
- [ ] Dagster UI local demo opens via `make dagster-refoundation-local-demo`.
- [ ] Full chain materialises using `config/dagster/full_synthetic_v1_run_config.yaml`.
- [ ] Required checks are visible in Dagster UI.
- [ ] Final curated table matches expected synthetic fixture.
- [ ] No legacy runtime imports are introduced in `src.randd_pipeline`.

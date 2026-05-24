# Local synthetic v1 readiness checklist (status review)

## Status legend

- **Ready for local validation**: artefact exists and is aligned, pending environment run.
- **Verified by test**: validated by automated test execution in the current Codex environment.
- **Requires Andy/local environment run**: must be executed in a dependency-complete local setup.
- **Deferred**: intentionally out of scope for local synthetic v1.

## 1) Scope declaration

- **Ready for local validation** — This declaration covers local synthetic v1 only.
- **Ready for local validation** — This declaration excludes GCP infrastructure/configuration.
- **Ready for local validation** — This declaration is not a production deployment sign-off.
- **Ready for local validation** — This declaration does not claim full legacy parity.
- **Ready for local validation** — CSV/Excel/API dissemination is not included yet.

## 2) Current implementation status

- **Ready for local validation** — Full synthetic chain is represented end-to-end in assets from `raw.full_responses` through `curated.rnd_statistics`.
- **Ready for local validation** — Major input/reference/intermediate/curated tables are represented in local demo definitions and run config.
- **Ready for local validation** — Asset checks are defined for chain stages and curated output.
- **Ready for local validation** — Final canonical output remains `curated.rnd_statistics`.
- **Ready for local validation** — Downstream dissemination strategy (table-first, exports later) is documented.

## 3) Local synthetic v1 validation commands

Run in a local environment with required dependencies:

```bash
make requirements-dev
pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py
make dagster-refoundation-local-demo
```

Then in Dagster UI:

1. Open the UI.
2. Use `config/dagster/full_synthetic_v1_run_config.yaml`.
3. Materialise the full chain.
4. Inspect required checks.
5. Confirm `curated/rnd_statistics` materialises.

## 4) Verification state in Codex environment (this review)

- **Requires Andy/local environment run** — `pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py` (skipped in Codex due to missing optional dependency: `pandas`).
- **Requires Andy/local environment run** — Dagster UI local demo via `make dagster-refoundation-local-demo`.
- **Requires Andy/local environment run** — Full chain materialisation using `config/dagster/full_synthetic_v1_run_config.yaml`.
- **Requires Andy/local environment run** — Required checks visibility and pass/fail inspection in Dagster UI.
- **Requires Andy/local environment run** — Final curated table reconciliation with synthetic expected output during local run.
- **Ready for local validation** — No new legacy runtime imports introduced as part of this readiness/status pass.

## 5) Intentionally deferred from local synthetic v1 declaration

- **Deferred** — Production raw ingestion.
- **Deferred** — Production Iceberg/catalog deployment.
- **Deferred** — GCP infrastructure.
- **Deferred** — Snapshot/release tagging.
- **Deferred** — API serving.
- **Deferred** — CSV/Excel exports.
- **Deferred** — Full legacy output parity.
- **Deferred** — Performance testing.
- **Deferred** — Operator training.

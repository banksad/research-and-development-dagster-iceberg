# Install and run the Dagster pipelines

This guide covers the local Dagster paths that are available in the current refoundation work. They use tiny synthetic fixtures and a local SQLite-backed PyIceberg catalog. They do **not** use production data, production credentials, or production object storage.

## What you can run today

There are two Dagster definitions modules:

- `src.randd_pipeline.definitions` is the production-facing lean refoundation module. It exposes the main refoundation assets and checks, but it does not wire demo-only CSV operational inputs.
- `src.randd_pipeline.local_demo_definitions` is the recommended local smoke/demo module. It adds the local synthetic `ops.manual_outliers` CSV helper and a local `TableStoreResource`, so the committed synthetic run config can materialise the full v1 chain.

For local end-to-end testing and UI exploration, use `src.randd_pipeline.local_demo_definitions` via the `make dagster-refoundation-local-demo` command.

## 1. Create and activate a virtual environment

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell, activate with:

```powershell
.venv\Scripts\Activate.ps1
```

## 2. Install dependencies

Install the combined legacy and refoundation development dependencies:

```bash
make requirements-dev
```

If you do not want to run the Make target, the equivalent dependency install is:

```bash
python -m pip install -U pip setuptools
python -m pip install -r requirements-dev.txt
pre-commit install
```

`requirements-dev.txt` includes the base project dependencies and `requirements-refoundation.txt`, which adds Dagster, the Dagster webserver, and PyIceberg support.

## 3. Run the synthetic end-to-end pipeline as a smoke test

Before opening the UI, run the synthetic v1 chain test:

```bash
pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py
```

This executes the local refoundation assets in process, writes to a temporary local warehouse, checks the expected tables exist, and validates the key downstream outputs.

## 4. Start Dagster for the local synthetic demo

Start the local Dagster UI with:

```bash
make dagster-refoundation-local-demo
```

This runs:

```bash
python -m dagster dev -m src.randd_pipeline.local_demo_definitions
```

Dagster will print the local URL in the terminal, usually `http://127.0.0.1:3000`.

## 5. Materialise the full synthetic v1 chain in the Dagster UI

1. Open the Dagster URL shown in the terminal.
2. Open the asset graph or Launchpad for the implicit asset job.
3. Paste or load the committed run config from `config/dagster/full_synthetic_v1_run_config.yaml`.
4. Materialise the selected assets, or materialise the full chain from `raw/full_responses` through `curated/rnd_statistics`.
5. Inspect materialisation events and asset checks in the run view and asset detail pages.

The run config uses these Dagster op names:

- `raw__full_responses`
- `intermediate__staged_responses`
- `ref__ultfoc_mapper`
- `ref__cell_number_mapper`
- `ops__manual_outliers`
- `ref__site_apportionment_factors`

Use the namespaced op keys above in Launchpad config, not legacy function-style names such as `raw_full_responses` or `staged_responses`.

## 6. Re-run safely

The local table store is deliberately non-mutating by default: assets fail if their target tables already exist unless overwrite behaviour is explicitly implemented for that pathway.

For repeated UI runs, either delete the default demo warehouse:

```bash
rm -rf .tmp/refoundation-ui-warehouse
```

or point the demo at a fresh warehouse:

```bash
RND_PIPELINE_LOCAL_WAREHOUSE=.tmp/refoundation-ui-warehouse-run-2 make dagster-refoundation-local-demo
```

## Other useful commands

Run the production-facing lean definitions module:

```bash
make dagster-refoundation-dev
```

Run the refoundation package tests only:

```bash
make test-randd-pipeline
```

## Current limitations

- The local demo uses synthetic fixture CSVs only.
- The implemented table-store mode is local SQLite-backed PyIceberg only.
- The current refoundation path is not a production deployment and is not a GCP deployment.
- No production endpoints, credentials, secrets, or real data are required.
- CSV and Excel dissemination remain downstream export concerns; the refoundation target treats managed tables as canonical persistence.

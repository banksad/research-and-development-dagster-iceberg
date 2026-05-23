# Refoundation development setup

## Dependency sets

- `requirements.txt` remains the legacy dependency set used by the current file-era pipeline and existing contributor workflows.
- `requirements-refoundation.txt` adds the minimal optional dependencies needed for lean Dagster/Iceberg refoundation development.
- `requirements-dev.txt` composes both sets for contributors working across legacy and refoundation surfaces.

## Developer commands

- Install combined dependencies:

  ```bash
  make requirements-dev
  ```

- Run the refoundation package tests only:

  ```bash
  make test-randd-pipeline
  ```

- Run Dagster against the future lean definitions module:

  ```bash
  make dagster-refoundation-dev
  ```

## Orchestration direction

- `src/randd_pipeline.definitions` is the target Dagster module for the lean refoundation architecture.
- `src/orchestration/dagster/definitions.py` is legacy wrapper scaffolding and should not be extended for new implementation work.

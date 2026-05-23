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

## Local Iceberg development/testing notes

- Local Iceberg tests use temporary SQLite-backed PyIceberg SQL catalogs under pytest temporary directories.
- The local SQL catalog mode is strictly for development/testing and is not the production persistence design.
- Production deployment is expected to use a lakehouse microservice/catalog-service configuration, likely REST-compatible.
- Docker packaging for Google Cloud / Artifact Registry is intended later in the rollout, but is not implemented in this change.
- No production endpoints, credentials, or secrets are stored in the repository for this setup.

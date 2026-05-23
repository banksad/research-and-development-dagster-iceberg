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
- Table creation is deliberately non-mutating by default: `create_table_from_dataframe(...)` should fail when a table already exists unless `overwrite=True` is passed explicitly.
- Replacement writes must be explicit via `overwrite=True`; row appends must be explicit via `append_dataframe(...)`.
- The local SQL catalog mode is strictly for development/testing and is not the production persistence design.
- Production deployment is expected to use a lakehouse microservice/catalog-service configuration, likely REST-compatible.
- Docker packaging for Google Cloud / Artifact Registry is intended later in the rollout, but is not implemented in this change.
- No production endpoints, credentials, or secrets are stored in the repository for this setup.

## TableStore resource boundary

- `src/randd_pipeline/resources.py::TableStoreResource` is the future Dagster resource boundary for table persistence.
- The currently implemented backing mode is `local_sql` only, intended for local development and test usage.
- `rest` / lakehouse-service configuration is represented for forward compatibility, but intentionally raises `NotImplementedError` in the current scaffold.
- Future lean Dagster assets should depend on `TableStoreResource` / `TableStore` abstractions rather than importing PyIceberg APIs directly.


## Synthetic fixtures and parity harness

- Synthetic migration fixtures live under `tests/fixtures/synthetic/`.
- Use these fixtures to run small, deterministic legacy-vs-refoundation comparisons during pathway migration.
- Each scenario should include concise metadata (`scenario.yaml`) and tiny, human-readable CSV extracts.
- Keep fixture data non-sensitive and obviously synthetic; never commit real or production data.

## First raw-input smoke path

- The first refoundation smoke path materialises the synthetic `basic_responses/raw_full_responses.csv` fixture into the local `TableStore` as `raw.full_responses`.
- Naming convention for this path is explicit and stable:
  - Python function name: `raw_full_responses`
  - Dagster asset key path: `raw/full_responses`
  - Iceberg table identifier: `raw.full_responses`
- The `raw_full_responses` Dagster smoke asset is deliberately non-mutating by default, so duplicate materialisation into the same table store fails until an explicit overwrite or partition strategy is designed.
- The smoke check then reads `raw.full_responses` back and compares it against the synthetic expected fixture.
- This proves fixture loading plus local table-store persistence boundaries only; it is not a staging migration and not a business/statistical logic migration.
- Follow-on assets should keep the same shape: implement a small domain/helper seam first, then add a thin Dagster asset wrapper over that seam.

## First contract-backed raw input checks

- The first contract-backed checks validate the materialised `raw.full_responses` table against `config/table_contracts/base.yaml`.
- These checks currently cover table existence, non-empty rows, and required-column presence for `raw.full_responses`.
- This remains input smoke/contract validation only; it is not staging migration and not business/statistical logic migration.


## First staging transmutation smoke path

- The first staging smoke asset materialises the clean staging transmutation seam to `intermediate.staged_responses`.
- This proves thin Dagster asset orchestration wired to domain seam execution and `TableStore` materialisation.
- It is intentionally **not** a full staging migration and **not** a `run_staging` port.
- Postcode validation, mapper loading, manual files, freezing, and construction concepts remain deliberately out of scope for this first vertical slice.

## Staged responses contract-backed checks

- `staged_responses` now has contract-backed checks for table existence, non-empty output, required columns, and unique grain on `reference + instance + survey_type + survey_year`.
- These remain smoke/contract checks over the clean transmutation seam.
- They are not full staging QA and do not cover postcode or mapper validation.

## First mapping foreign-ownership smoke path

- The first mapping smoke asset materialises the clean foreign-ownership seam to `intermediate.mapped_responses`.
- This proves Dagster asset orchestration, clean domain seam execution, and `TableStore` persistence wiring for mapping.
- This is intentionally **not** a full mapping migration.
- Postcode/ITL mapping, PG conversion, cell-number mapping, PNP mapping, NI mapping, mapper file loading framework, and mapping QA outputs are deliberately out of scope for this thin vertical slice.
- `mapped_responses` now has contract-backed checks for table existence, non-empty output, required columns, unique grain (`reference+instance+survey_type+survey_year`), and populated `ultfoc`.
- These remain smoke/contract checks over the clean foreign-ownership seam; they are not full mapping QA and do not cover postcode/ITL, PG, cell-number, PNP, or NI mapping.

## Staging-to-mapping chain smoke test

- A synthetic in-process Dagster smoke test now validates the first connected refoundation slice: `staged_responses -> intermediate.staged_responses -> mapped_responses -> intermediate.mapped_responses`, including staged and mapped asset checks.
- This remains synthetic fixture coverage and is **not** a full pipeline migration.
- The following remain out of scope for this smoke path: raw snapshot ingestion, postcode/ITL mapping, product-group conversion, cell-number mapping, PNP area mapping, NI logic, imputation, estimation, and final outputs.

## Mapping smoke ref-table note

- Foreign ownership mapper is now represented as explicit `ref/ultfoc_mapper` (`ref.ultfoc_mapper`) in the mapping smoke slice.
- `mapped_responses` now reads mapper data from `TableStore` (`ref.ultfoc_mapper`) instead of direct mapper CSV config.
- This is still a thin seam slice and not a general mapper-loading framework.

- `ref.ultfoc_mapper` now has contract-backed checks for table existence, non-empty output, required columns, and unique `ruref`.
- These are reference-input checks for the first foreign ownership mapping seam only.
- They are not a general reference-data validation framework.

## Mapping table-shape ADR note

- Mapping seam-level tables are currently useful development slices during refoundation.
- ADR `docs/architecture/0009-mapping-table-shape.md` proposes that v1 downstream consumption should converge on canonical `intermediate.mapped_responses`, with seam-level mapped tables retained only when explicitly justified as temporary debug/checkpoint assets.

## Cell-number seam note

- A first clean cell-number seam now exists via `ref/cell_number_mapper` and `intermediate/cell_number_mapped_responses` using synthetic fixtures and canonicalised mapper columns.
- This is still not a full mapping migration: postcode/ITL, PG conversion, PNP, NI, and broader `run_mapping` behaviour remain out of scope.

- Cell-number seam checks now include contract-backed checks for `ref/cell_number_mapper` (existence, non-empty, required columns, unique `cellnumber`, inclusive range 1..817) and `intermediate/cell_number_mapped_responses` (existence, non-empty, required columns, unique grain, mapped metadata populated where `cellno` is non-null).
- This remains a narrow cell-number seam quality gate and is not full mapping QA coverage.

## Canonical mapped-output contract transition note

- `intermediate.mapped_responses` is now the documented canonical mapped-output contract for the current implemented v1 mapping scope.
- Runtime consolidation is now implemented: `mapped_responses` composes foreign-ownership and cell-number seams and materialises canonical `intermediate.mapped_responses`.
- `intermediate.cell_number_mapped_responses` remains a temporary/checkpoint mapped table during this transition unless explicitly retained as a debug asset.

### Follow-up status update

- `mapped_responses` is now the production-facing v1 mapping output.
- `mapped_responses_cell_number_mapping_complete` is registered in the default Dagster definitions for canonical `intermediate.mapped_responses`.
- `cell_number_mapped_responses` is no longer required in the default production-facing mapping chain and is treated as checkpoint/debug-only pending final deprecation/removal.

## First imputation seam note

- A first minimal imputation seam now exists from `intermediate.mapped_responses` to `intermediate.imputed_responses`.
- The implemented seam is intentionally narrow: simple class-mean/TMI-style imputation for a synthetic target column with explicit markers.
- This is not a full legacy imputation migration.
- MoR, backdata, manual trimming, short-form expansion, and carry-forward behaviours remain out of scope in this slice.
- `intermediate.imputed_responses` is now the next production-facing stage after `intermediate.mapped_responses` for the lean v1 chain.

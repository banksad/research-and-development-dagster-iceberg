# Local synthetic v1 demo

## Purpose

This local demo shows the lean refoundation v1 pipeline running end-to-end with synthetic data only, from raw input seams to the canonical curated statistics table.

## Stage-by-stage flow in plain English

1. `raw.full_responses`: minimal synthetic raw response records are materialised as the raw input seam.
2. `intermediate.staged_responses`: the current synthetic staging asset uses contributor and response-long CSV inputs to exercise the transmutation seam and appears downstream of raw in the Dagster graph.
3. `intermediate.mapped_responses`: mapping seams add foreign ownership and cell-number context.
4. `intermediate.imputed_responses`: minimal class-mean imputation fills configured target gaps.
5. `intermediate.outlier_adjusted_responses`: manual outlier overrides are applied where supplied.
6. `intermediate.estimated_responses`: minimal estimation weights are calculated.
7. `intermediate.site_apportioned_responses`: estimated values are split to sites using explicit factors.
8. `curated.rnd_statistics`: canonical statistical output is produced for downstream dissemination.

Future production ingestion may replace these local CSV staging inputs with managed raw tables/resources.

## Visible checks

The smoke path validates checks across major seams, including:
- raw full responses checks,
- ULTFOC mapper checks,
- cell-number mapper checks,
- staged responses checks,
- mapped responses checks,
- imputed responses checks,
- outlier-adjusted responses checks,
- estimated responses checks,
- site apportionment factors and site-apportioned response checks,
- curated output checks.

## Where manual operational inputs appear

- `ops.manual_outliers`: manual operator/methodologist intervention for outlier decisions.
  - Local demo definitions intentionally include this CSV helper asset so the committed full synthetic v1 run config can materialise `ops.manual_outliers` from fixtures.
- `ref.site_apportionment_factors`: explicit site split factors used to allocate estimated values.

## Final curated output meaning

`curated.rnd_statistics` is the canonical statistical output table for this v1 seam. It is the source-of-truth statistical product in the refoundation pipeline.

## Why this is table-first (not CSV/Excel)

The refoundation operating model treats Iceberg-style managed tables as canonical persistence. CSV/Excel are downstream dissemination artefacts, not truth-bearing storage.

## How this supports future dissemination

By stabilising curated outputs as canonical tables, future dissemination layers can read from controlled table/view contracts:
- API access layer (future),
- export jobs for CSV/Excel (future),
- other controlled delivery channels.

## Known limitations

- Synthetic data only.
- Local table store only.
- Not a GCP deployment.
- Not performance tested.
- Not full parity with all legacy outputs.
- No CSV/Excel/API dissemination in this demo.


## How to inspect this in Dagster UI

1. Start the local demo server:

   ```bash
   make dagster-refoundation-local-demo
   ```

2. Open Dagster in your browser and launch a run using `config/dagster/full_synthetic_v1_run_config.yaml`.
3. Materialise the full synthetic chain from `raw/full_responses` to `curated/rnd_statistics`.
4. Review asset checks in the run and asset detail pages.
5. If you intentionally break a fixture, inspect failed checks to confirm check visibility and failure diagnostics.
6. Confirm the final output asset is `curated/rnd_statistics` (`curated.rnd_statistics`).

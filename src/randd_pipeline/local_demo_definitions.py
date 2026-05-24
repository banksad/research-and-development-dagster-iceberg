"""Local synthetic v1 Dagster definitions for UI demo and readiness checks.

This module is local-development/demo only. It mirrors production-facing refoundation
assets/checks while wiring an explicit local TableStore resource so operators can run
and inspect the synthetic v1 chain in Dagster UI without creating ad-hoc wrappers.
"""

from __future__ import annotations

import os

from dagster import Definitions

from src.randd_pipeline.definitions import _asset_checks, _assets
from src.randd_pipeline.resources import TableStoreResource

_LOCAL_WAREHOUSE_ENV = "RND_PIPELINE_LOCAL_WAREHOUSE"
_DEFAULT_LOCAL_WAREHOUSE = ".tmp/refoundation-ui-warehouse"


def _local_demo_warehouse() -> str:
    return os.getenv(_LOCAL_WAREHOUSE_ENV, _DEFAULT_LOCAL_WAREHOUSE)


defs = Definitions(
    assets=_assets,
    asset_checks=_asset_checks,
    resources={
        "table_store": TableStoreResource(
            catalog_name="local-refoundation-ui",
            catalog_type="local_sql",
            warehouse=_local_demo_warehouse(),
        )
    },
)

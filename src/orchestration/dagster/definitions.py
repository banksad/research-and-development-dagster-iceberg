"""LEGACY Dagster wrapper scaffolding (deprecated; not target architecture).

This module is intentionally retained as a temporary compatibility scaffold only:
- It mirrors legacy `src.pipeline` stage boundaries via placeholders.
- It is NOT the implementation path for the lean Dagster/Iceberg refoundation.
- Do not extend this file for new pipeline implementation work.
- Future Dagster implementation should live under `src/randd_pipeline/`.

No statistical/business logic is implemented or changed in this module.
"""

from __future__ import annotations

from importlib import import_module
from typing import Callable

try:
    from dagster import Definitions, asset
except ModuleNotFoundError:  # pragma: no cover - allows import-level tests without dagster installed
    class Definitions:  # type: ignore[override]
        """Lightweight fallback so this module remains importable without Dagster."""

        def __init__(self, assets: list[Callable]):
            self.assets = assets

    def asset(*_args, **_kwargs):
        def _decorator(fn: Callable):
            return fn

        return _decorator


def _load_callable(module_name: str, fn_name: str) -> Callable:
    """Resolve referenced stage callables lazily to avoid runtime/env coupling."""
    return getattr(import_module(module_name), fn_name)


@asset(name="staging")
def staging_asset() -> str:
    _ = _load_callable("src.staging.staging_main", "run_staging")
    return "staging wrapper placeholder"


@asset(name="freezing")
def freezing_asset() -> str:
    _ = _load_callable("src.freezing.freezing_main", "run_freezing")
    return "freezing wrapper placeholder"


@asset(name="ni")
def ni_asset() -> str:
    _ = _load_callable("src.northern_ireland.ni_main", "run_ni")
    return "ni wrapper placeholder"


@asset(name="construction")
def construction_asset() -> str:
    _ = _load_callable("src.construction.construction_main", "run_construction")
    return "construction wrapper placeholder"


@asset(name="mapping")
def mapping_asset() -> str:
    _ = _load_callable("src.mapping.mapping_main", "run_mapping")
    return "mapping wrapper placeholder"


@asset(name="imputation")
def imputation_asset() -> str:
    _ = _load_callable("src.imputation.imputation_main", "run_imputation")
    return "imputation wrapper placeholder"


@asset(name="outlier")
def outlier_asset() -> str:
    _ = _load_callable("src.outlier_detection.outlier_main", "run_outliers")
    return "outlier wrapper placeholder"


@asset(name="estimation")
def estimation_asset() -> str:
    _ = _load_callable("src.estimation.estimation_main", "run_estimation")
    return "estimation wrapper placeholder"


@asset(name="site_apportionment")
def site_apportionment_asset() -> str:
    _ = _load_callable("src.site_apportionment.site_apportionment_main", "run_site_apportionment")
    return "site apportionment wrapper placeholder"


@asset(name="outputs")
def outputs_asset() -> str:
    _ = _load_callable("src.outputs.outputs_main", "run_outputs")
    return "outputs wrapper placeholder"


LEGACY_WRAPPER_SCAFFOLD_NOTICE = (
    "Deprecated wrapper scaffold: do not extend this module for the lean refoundation; "
    "implement new Dagster assets/definitions under src/randd_pipeline/."
)


defs = Definitions(
    assets=[
        staging_asset,
        freezing_asset,
        ni_asset,
        construction_asset,
        mapping_asset,
        imputation_asset,
        outlier_asset,
        estimation_asset,
        site_apportionment_asset,
        outputs_asset,
    ]
)

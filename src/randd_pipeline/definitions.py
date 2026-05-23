"""Minimal importable Dagster definitions for the lean refoundation package.

This module intentionally exposes only a minimal placeholder Definitions object.
It must remain importable regardless of whether Dagster is installed.
"""

from __future__ import annotations

from typing import Callable

try:
    from dagster import Definitions
    from src.randd_pipeline.assets.inputs import raw_full_responses
except ModuleNotFoundError:  # pragma: no cover - fallback for environments without dagster
    class Definitions:  # type: ignore[override]
        """Lightweight fallback so this module remains importable without Dagster."""

        def __init__(self, assets: list[Callable] | None = None):
            self.assets = assets or []


    _assets: list[Callable] = []
else:
    _assets = [raw_full_responses]


defs = Definitions(assets=_assets)

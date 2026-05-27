from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from pandas.testing import assert_frame_equal


def fixture_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures" / "synthetic"


def scenario_path(scenario_id: str) -> Path:
    return fixture_root() / "scenarios" / scenario_id


def load_scenario_csv(scenario_id: str, filename: str) -> pd.DataFrame:
    return pd.read_csv(scenario_path(scenario_id) / filename)


def load_scenario_metadata(scenario_id: str) -> dict:
    with (scenario_path(scenario_id) / "scenario.yaml").open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def assert_frame_equal_sorted(left: pd.DataFrame, right: pd.DataFrame, sort_by: list[str]) -> None:
    left_sorted = left.sort_values(sort_by).reset_index(drop=True)
    right_sorted = right.sort_values(sort_by).reset_index(drop=True)
    for col in left_sorted.columns.intersection(right_sorted.columns):
        if left_sorted[col].isna().all() and right_sorted[col].isna().all():
            left_sorted[col] = left_sorted[col].astype("object")
            right_sorted[col] = right_sorted[col].astype("object")
    assert_frame_equal(left_sorted, right_sorted, check_like=False)

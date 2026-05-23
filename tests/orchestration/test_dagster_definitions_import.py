"""Import-level test coverage for Dagster bootstrap definitions."""

from src.orchestration.dagster import definitions


EXPECTED_ASSET_NAMES = {
    "staging",
    "freezing",
    "ni",
    "construction",
    "mapping",
    "imputation",
    "outlier",
    "estimation",
    "site_apportionment",
    "outputs",
}


def _asset_name(asset_obj):
    """Return name for either Dagster AssetsDefinition or fallback function."""
    if hasattr(asset_obj, "key"):
        return asset_obj.key.path[-1]
    return getattr(asset_obj, "__name__", "")


def test_definitions_module_loads_and_contains_expected_assets():
    assets = getattr(definitions.defs, "assets", [])
    assert assets, "Definitions should expose bootstrap assets"

    asset_names = {_asset_name(asset) for asset in assets}

    if "staging_asset" in asset_names:
        # Fallback mode when Dagster is not installed.
        asset_names = {
            name.replace("_asset", "") for name in asset_names if name.endswith("_asset")
        }

    assert EXPECTED_ASSET_NAMES == asset_names

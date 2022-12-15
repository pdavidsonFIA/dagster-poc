import pandas as pd
from dagster import asset, AssetsDefinition, SourceAsset, AssetKey

from ..ops.ops import sample_data, concat_samples


# @asset(code_version=version)
@asset()
def asset_sample1() -> pd.DataFrame:
    return sample_data()


@asset
def asset_sample2() -> pd.DataFrame:
    return sample_data()


@asset
def concat_assets(asset_sample1, asset_sample2) -> pd.DataFrame:
    return concat_samples([asset_sample1, asset_sample2])


# graph_asset = AssetsDefinition.from_graph(graph_multi_sample)


sourcesample = SourceAsset(key=AssetKey("sourcesample"))


@asset
def concat_assets_incl_source(asset_sample1, asset_sample2, sourcesample) -> pd.DataFrame:
    return concat_samples([asset_sample1, asset_sample2, sourcesample])


@asset(required_resource_keys={"globals"})
def conf_asset():
    return sample_data()

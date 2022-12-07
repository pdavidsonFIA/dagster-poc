import pandas as pd
from ..ops.ops import sample_data, concat_samples
from dagster import asset


@asset
def asset_sample1() -> pd.DataFrame:
    return sample_data()


@asset
def asset_sample2() -> pd.DataFrame:
    return sample_data()


@asset
def concat_assets(asset_sample1, asset_sample2) -> pd.DataFrame:
    return concat_samples(asset_sample1, asset_sample2)

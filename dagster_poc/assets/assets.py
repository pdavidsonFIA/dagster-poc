import pandas as pd
from dagster import asset

from ..ops.ops import concat_samples
from ..common import sample_data


# @asset(code_version=version)
@asset()
def asset_sample1() -> pd.DataFrame:
    return sample_data()


@asset
def asset_sample2() -> pd.DataFrame:
    return sample_data()


@asset
def concat_assets(asset_sample1, asset_sample2) -> pd.DataFrame:
    print('Concat called')
    return concat_samples([asset_sample1, asset_sample2])


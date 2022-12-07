import pandas as pd
from ..ops.ops import sample_data, concat_samples
from dagster import asset, define_asset_job
from ..resources import my_io_manager_int
from ..resources.simple_config import simplified_config

@asset(
    # config_schema=simplified_config,
    # config_schema={"simplified_param": int},
    io_manager_def=my_io_manager_int
)
def asset_sample1() -> pd.DataFrame:
    return sample_data()


# @asset(io_manager_def=my_io_manager_int)
# def asset_sample2() -> pd.DataFrame:
#     return sample_data()
#
#
# @asset(io_manager_def=my_io_manager_int)
# def concat_assets(asset_sample1, asset_sample2) -> pd.DataFrame:
#     return concat_samples(asset_sample1, asset_sample2)
#

all_assets_job = define_asset_job(name="all_assets_job") #, config=simplified_config)
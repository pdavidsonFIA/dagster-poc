"""
This generates sample data - multiple identical assets

"""

import pandas as pd
from dagster import (
    asset,
    List,
    MultiPartitionsDefinition,
)
from ..common import sample_data
from ..partitions import all_scenarios, all_months


@asset(partitions_def=all_scenarios, group_name='part', io_manager_key='part_assets')
def raw_data_scenarios() -> pd.DataFrame:
    return sample_data()


@asset(partitions_def=all_months, group_name='part', io_manager_key='part_assets')
def raw_data_monthly() -> pd.DataFrame:
    return sample_data()


asset_keys = ["source1", "source2", "source3"]


def source_data_factory(asset_keys: List[str]):
    assets = []
    # for i, key in enumerate(asset_keys):
    for key in asset_keys:
        @asset(name=key,
               partitions_def=MultiPartitionsDefinition({
                   'rep_date': all_months,
                   'xdata_scenario': all_scenarios}),
               group_name='part', io_manager_key='part_assets')
        def raw_data_monthly_scenarios() -> pd.DataFrame:
            return sample_data()

        assets.append(raw_data_monthly_scenarios)
    return assets


raw_data = source_data_factory(asset_keys)

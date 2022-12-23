import pandas as pd
from dagster import asset
from ..common import sample_data
from ..partitions import all_scenarios


@asset(partitions_def=all_scenarios, group_name='part', io_manager_key='part_assets')
def partition_sample() -> pd.DataFrame:
    return sample_data()


@asset(partitions_def=all_scenarios, group_name='part', io_manager_key='part_assets')
def partition_samplestep(partition_sample) -> pd.DataFrame:
    return partition_sample

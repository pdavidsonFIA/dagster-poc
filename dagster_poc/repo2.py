"""
Standalone module illustrating separate code space

"""

import os
from datetime import date
import pathlib
import pandas as pd
from dagster import (
    Definitions,
    asset, define_asset_job,
    config_mapping,
    SourceAsset, AssetIn,
    IOManager, io_manager,
    List,
)


class MyPartitionedIOManager(IOManager):

    def __init__(self, base_dir):
        print('IO Manager initialized')
        if not os.path.isdir(base_dir):
            os.makedirs(base_dir)
        self.base_dir = base_dir

    def _get_path(self, context, partition_override=None) -> str:
        if partition_override is not None:
            print(f'Asset keys: {context.asset_key.path}')
            output_path = os.path.join(self.base_dir, *context.asset_key.path, partition_override)
        elif context.has_partition_key:
            print(f'Asset keys: {context.asset_key.path}')
            print(f'Part keys: {context.asset_partition_key}')
            output_path = os.path.join(self.base_dir, *context.asset_key.path, *context.asset_partition_key)
        else:
            output_path = os.path.join(self.base_dir, *context.asset_key.path)
        pathlib.Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        return output_path

    def handle_output(self, context: "OutputContext", obj: pd.DataFrame):
        obj.to_pickle(self._get_path(context))

    def load_input(self, context: "InputContext"):
        partition_override = context.step_context.op_config.get('partition_override')
        return pd.read_pickle(self._get_path(context, partition_override))


@io_manager(config_schema={"base_dir": str})
def my_fs_part_manager(init_context) -> MyPartitionedIOManager:
    return MyPartitionedIOManager(base_dir=init_context.resource_config["base_dir"])


def sample_data() -> pd.DataFrame:
    print("Sample regenerated")
    df = {
        1: {'mgroup_id': 1, 'd_from': date(2020, 1, 1), 'income': 100},
        2: {'mgroup_id': 2, 'd_from': date(2020, 1, 1), 'income': 100},
        3: {'mgroup_id': 3, 'd_from': date(2020, 1, 1), 'income': 100},
        4: {'mgroup_id': 4, 'd_from': date(2020, 1, 1), 'income': 100},
    }

    df = pd.DataFrame.from_dict(df, orient='index')

    df = df.astype({'d_from': 'datetime64[ns]', 'income': 'float64'})
    return df


@asset()
def repo2_asset():
    asset1 = sample_data()
    return asset1


repo1_asset = SourceAsset(key='asset_sample1')
repo1_partasset = SourceAsset(key='partition_sample')


@asset(ins={'upstream': AssetIn(key='partition_sample')})
def non_part_asset(context, upstream):
    try:
        context.log.info(f'Op Config inside asset: {context.op_config}')
    except:
        pass
    return upstream


part_nodes = ['non_part_asset']


def config_partition_factory(outputs: List[str]):
    @config_mapping(config_schema={'partition_override': str})
    def asset_part_config(val):
        config_for_input_assets = {'ops': {
            'non_part_asset': {
                'config': {'partition_override': val['partition_override']
                           }}}}
        return config_for_input_assets

    return asset_part_config


conf = config_partition_factory(part_nodes)
part_job = define_asset_job(name='part_job2', selection=part_nodes, config=conf)

local_dev_folder = str(pathlib.Path(__file__).parents[2].joinpath('dev').resolve())
resource_defs = {'io_manager': my_fs_part_manager.configured(
    config_or_config_fn={'base_dir': local_dev_folder})}
repo2 = Definitions(
    assets=[repo2_asset, repo1_asset, repo1_partasset, non_part_asset],
    resources=resource_defs,
    jobs=[part_job]
)

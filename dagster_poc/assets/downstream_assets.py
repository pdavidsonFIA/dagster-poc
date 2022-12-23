"""
This takes upstream assets, and (when run via job) selects specific upstream partition for downstream output

"""

import os
import pandas as pd
from dagster import (
    asset,AssetIn,
    List,
    MultiPartitionsDefinition,
config_mapping,
define_asset_job,
)
from ..common import sample_data
from ..partitions import all_scenarios, all_months, all_params
from ..ops.ops import concat_samples


source_asset_keys = ["source1", "source2", "source3"]
processed_asset_keys = ["proc1", "proc2", "proc3"]
# testing Iterator
# for i in zip(source_asset_keys, processed_asset_keys):
#     print(f'source: {i[0]}; proc: {i[1]}')
asset_keys = list(zip(source_asset_keys, processed_asset_keys))


def procesed_data_factory(asset_keys: List[str]):
    assets = []
    # for i, key in enumerate(asset_keys):
    for key in asset_keys:
        @asset(name=key[1],
               ins={'source_asset': AssetIn(key[0])},
               # partitions_def=all_params,
               group_name='part',
               io_manager_key='part_assets')
        def process_data(context, source_asset: pd.DataFrame) -> pd.DataFrame:
            return source_asset

        assets.append(process_data)
    return assets


from ..resources.configs import load_conf_from_csv


def downstream_config_factory(outputs: List[str]):
    """
    This takes a param set id and creates upstream partition selection keys.
    :param outputs:
    :return:
    """
    @config_mapping(config_schema={"param_id": int})
    def ops_output_config_factory(val):
        conf = load_conf_from_csv(val["param_id"])
        workspace_root = os.path.join(conf.get('run_type'), conf.get('rep_date'), conf.get('nickname'))
        ops_output_config_schema = {}

        xdata_scenario = conf.get('xdata_scenario')
        rep_date = conf.get('rep_date')
        # Re-align to use 1st of month for data as that's how dagster keys work
        rep_date = f'{rep_date[:-2]}01'
        part_key = f"{rep_date}|{xdata_scenario}"
        for output in outputs:
            # output_path = os.path.join(workspace_root, 'result', f"{output}.pkl")
            # This works for confiured
            # {'outputs': {'result': {'output_path': output_path}}}
            ops_output_config_schema[output] = {
                'config': {
                    'partition_override': part_key}}
        return {"ops": ops_output_config_schema}
    return ops_output_config_factory


proc_data = procesed_data_factory(asset_keys)


@asset(ins={k: AssetIn(k) for k in processed_asset_keys}, group_name='part',io_manager_key='part_assets')
def concat_downstream(**proc_data) -> pd.DataFrame:
    #proc data is a dict of dataframes
    print('Concat downstream called')
    # processed_data = []
    # for k, v in proc_data:
    #     processed_data.append(v)
    return concat_samples(list(proc_data.values()))


downstream_conf = downstream_config_factory(processed_asset_keys)


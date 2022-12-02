import os
import pathlib
import pandas as pd
from dagster import IOManager, io_manager


local_folder = pathlib.Path(__file__).parents[3].joinpath('sample_output')


class MyIOManager(IOManager):

    def __init__(self, base_dir):
        print("Base dir: " + base_dir)
        target_dir = os.path.join(local_folder, base_dir)
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)
        self._base_dir = target_dir

    def _get_path(self, context) -> str:
        op_name = context.op_def.name
        context.log.info("Op name: " + op_name)
        # return "/".join(asset_key)
        return os.path.join(self._base_dir, op_name + '.pkl')

    def handle_output(self, context, obj: pd.DataFrame):
        target_path = self._get_path(context)
        context.log.info("Output handlded: " + target_path)
        obj.to_pickle(target_path)

    def load_input(self, context):
        target_path = self._get_path(context)
        context.log.info("Input loaded: " + target_path)
        return pd.read_pickle(target_path)


@io_manager(config_schema={'base_dir': str})
def my_io_manager(init_context):
    return MyIOManager(init_context.resource_config.get('base_dir'))
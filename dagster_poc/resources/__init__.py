import os
import pathlib
import pandas as pd
from dagster import IOManager, io_manager

local_folder = pathlib.Path(__file__).parents[3].joinpath('sample_output')


def get_path_from_id(run_id: int) -> str:
    return str(local_folder.joinpath('dagster' + str(run_id)).resolve())


class MyIOManager(IOManager):

    def __init__(self, base_dir):
        if not os.path.isdir(base_dir):
            os.makedirs(base_dir)
        self.base_dir = base_dir

    def _get_path(self, output_context) -> str:

        file_name = f"{output_context.step_key}_{output_context.name}.pkl"
        return os.path.join(self.base_dir, file_name)

    def handle_output(self, context, obj: pd.DataFrame):
        target_path = self._get_path(context)
        context.log.info("Output handled: " + target_path)
        if obj is not None:
            obj.to_pickle(target_path)

    def load_input(self, context: "InputContext"):
        target_path = self._get_path(context.upstream_output)
        context.log.info("Input loaded: " + target_path)
        return pd.read_pickle(target_path)


@io_manager(config_schema={"simplified_param": int})
def my_io_manager_int(init_context):
    val = init_context.resource_config.get("simplified_param")
    init_context.log.info(f"Param id: {val}")

    folder = get_path_from_id(val)
    init_context.log.info(f"Base dir configured to: {folder}")

    return MyIOManager(base_dir=folder)



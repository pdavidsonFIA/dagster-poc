import os
import pandas as pd
from dagster import IOManager, io_manager
import pathlib

#
# def get_path_from_id(run_id: int) -> str:
#     return str(local_folder.joinpath('dagster' + str(run_id)).resolve())


class MyIOManager(IOManager):

    # def __init__(self, config):
    def __init__(self, base_dir):
        # base_dir = config["base_dir"]
        if not os.path.isdir(base_dir):
            os.makedirs(base_dir)
        self.base_dir = base_dir

    def _get_path(self, output_context) -> str:

        # rep_date = output_context.config['rep_date']
        # output_context.log.info(f"Asset key: {output_context.asset_key}")
        # output_context.log.info(f"Step key: {output_context.step_key}")
        # output_context.log.info(f"Name: {output_context.name}")
        # # output_context.log.info(output_context.asset_info)
        try:
            output_context.log.info(f'Config: {output_context.config}')
        except:
            pass
        output_path = os.path.join(self.base_dir, output_context.config['output_path'])
        output_context.log.info(f'Output path: {output_path}')
        # output_path = os.path.join(output_folder,  output_context.config['target_file'])
        pathlib.Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        # file_name = f"{output_context.step_key}.pkl"
        # if output_context.step_key is not None:
        if output_context.step_key != 'none':
            file_name = f"{output_context.step_key}_{output_context.name}.pkl"
        else:
            file_name = f"{output_context.name}.pkl"
        # return os.path.join(self.base_dir, file_name)
        return output_path

    def handle_output(self, context, obj: pd.DataFrame):
        target_path = self._get_path(context)
        context.log.info("Output handled: " + target_path)
        if obj is not None:
            obj.to_pickle(target_path)

    def load_input(self, context: "InputContext"):
        target_path = self._get_path(context.upstream_output)
        context.log.info("Input loaded: " + target_path)
        return pd.read_pickle(target_path)


@io_manager(config_schema={"base_dir": str}, output_config_schema={'output_path': str})
def my_fs_manager(init_context) -> MyIOManager:
    return MyIOManager(base_dir=init_context.resource_config["base_dir"])



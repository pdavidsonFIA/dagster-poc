from dagster import config_mapping
import pathlib

from dagster_poc.resources import get_path_from_id

local_folder = pathlib.Path(__file__).parents[3].joinpath('sample_output')


@config_mapping(config_schema={"simplified_param": int})
def simplified_config(val):
    folder = get_path_from_id(val["simplified_param"])
    return {
        "resources": {"io_manager": {"config": {"base_dir": folder}}}
    }


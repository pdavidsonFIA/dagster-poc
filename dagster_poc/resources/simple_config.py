from dagster import config_mapping


def get_path_from_id(run_id: int) -> str:
    return 'dagster' + str(run_id)


@config_mapping(config_schema={"simplified_param": int})
def simplified_config(val):
    folder = get_path_from_id(val["simplified_param"])
    return {
        "resources": {"io_manager": {'config': {"base_dir": folder}}}
    }


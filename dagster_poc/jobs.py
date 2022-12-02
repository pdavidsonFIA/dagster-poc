from dagster import config_mapping, job
# from .resources import local_io_manager
from .resources import my_io_manager
from dagster_poc.ops.sample_data import generate_aggregated_samples


def get_path_from_id(run_id: int) -> str:
    if run_id == 1:
        return r'G:\Shared drives\wefox-act\dev\2022\ws\20220930_act-757_piping\dagster1'
    else:
        return r'G:\Shared drives\wefox-act\dev\2022\ws\20220930_act-757_piping\dagster0'


@config_mapping(config_schema={"simplified_param": int})
def simplified_config(val):
    folder = get_path_from_id(val["simplified_param"])
    return {
        "resources": {"io_manager": {'config': {"base_dir": folder}}}
    }


@job(config=simplified_config,
     resource_defs={'io_manager': my_io_manager}
     )
def aggregate_job():
    generate_aggregated_samples()


if __name__ == "__main__":
    # Will log "config_param: stuff"
    aggregate_job.execute_in_process(
        run_config={"simplified_param": 0}
    )
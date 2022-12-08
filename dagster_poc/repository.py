# import sys, pathlib
#
# project_root = pathlib.Path(__file__).parents[1].resolve()
# print(project_root)
# sys.path.append(project_root)

from dagster import repository, with_resources

from .resources import my_io_manager_int
from .jobs import (
    job_int_param,
    job_from_graph,
    job_from_graph_stacked,
    job_multi_sample,
    job_pipe,
    all_assets_job,

)

from dagster import load_assets_from_package_module
from . import assets

sample_assets = load_assets_from_package_module(package_module=assets, group_name='sample')


@repository
def dagster_poc():
    return [
        job_int_param,
        job_from_graph,
        job_from_graph_stacked,
        job_multi_sample,
           job_pipe,
        *with_resources(sample_assets, resource_defs={'io_manager': my_io_manager_int}),
        all_assets_job,
    ]


if __name__ == "__main__":

    result = job_multi_sample.execute_in_process(run_config={"resources": {"io_manager": {"config": {"simplified_param": 1}}}})
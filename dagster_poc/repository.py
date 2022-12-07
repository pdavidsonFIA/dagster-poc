from dagster import repository, with_resources

from .resources import my_io_manager_int
from .jobs import (
    job_int_param,
    job_from_graph,
    job_from_graph_stacked,
    job_multi_sample,
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
        *with_resources(sample_assets, resource_defs={'io_manager': my_io_manager_int}),
        all_assets_job,
    ]

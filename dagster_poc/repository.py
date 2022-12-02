import os.path

from dagster import (
    load_assets_from_package_module,
    repository,
    with_resources
)

from dagster_poc import assets
# from .assets.pandas_flow import subfolder
from .resources import my_io_manager
from .jobs import aggregate_job

# def get_subfolder(id: int):
#     if id == 1:
#         return r'20220930_act-757_piping'
#     else:
#         return 'failed'

@repository
def dagster_poc():
    # resource_defs = {'io_manager': my_io_manager}
    # resource_defs = {'io_manager': configured_io}
    repo = []
    # repo += [*with_resources(load_assets_from_package_module(assets), resource_defs=resource_defs)]
    repo += [aggregate_job]
    return repo






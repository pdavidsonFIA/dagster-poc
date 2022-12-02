from dagster import repository
from .jobs import aggregate_job #, diamond


@repository
def dagster_poc():
    repo = []
    # repo += [*with_resources(load_assets_from_package_module(assets), resource_defs=resource_defs)]
    repo += [aggregate_job] #, diamond]
    return repo







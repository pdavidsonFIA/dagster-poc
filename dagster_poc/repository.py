from dagster import repository, with_resources

from .resources import  my_io_manager_int
from .jobs import (
job_int_param,
job_from_graph,
job_from_graph_stacked
)
from .assets.assets import (
    asset_sample1,
    # asset_sample2,
    # concat_assets,
    all_assets_job
)
@repository
def dagster_poc():
    return [
        job_int_param,
        job_from_graph,
        job_from_graph_stacked,
        asset_sample1,
        # asset_sample2,
        # concat_assets,
        all_assets_job,
        # *with_resources([asset_sample1,
        #                  # asset_sample2, concat_assets
        #                  ],resource_defs={'io_manager': my_io_manager_int})
            ]
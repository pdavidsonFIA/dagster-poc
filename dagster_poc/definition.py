import os
import pathlib
from dagster import define_asset_job, Definitions, fs_io_manager

from .resources import my_fs_manager, my_fs_part_manager
from .resources.configs import (
    config_mapping_factory
)
from .ops.ops import (
    graph_samples,
    graph_multi_sample,
    partitions_graph
)
from .assets.assets import (
    asset_sample1,
    asset_sample2,
    concat_assets,
)
from .assets.partition_assets import partition_sample, partition_samplestep
from .assets.source_data import raw_data_scenarios, raw_data_monthly, raw_data
from .assets.downstream_assets import proc_data, downstream_conf, processed_asset_keys

local_dev_folder = str(pathlib.Path(__file__).parents[2].joinpath('dev').resolve())
local_prod_folder = str(pathlib.Path(__file__).parents[2].joinpath('prod').resolve())
my_local_def_fs_manager = my_fs_manager.configured({'base_dir': local_dev_folder})
my_local_prod_fs_manager = my_fs_manager.configured({'base_dir': local_prod_folder})
resource_defs_by_deployment_name = {
    "prod": {'io_manager': my_local_prod_fs_manager},
    "staging": {'io_manager': fs_io_manager.configured(config_or_config_fn={'base_dir': local_dev_folder}),
                'part_assets': my_fs_part_manager.configured(config_or_config_fn={'base_dir': local_dev_folder})},
    "local": {'io_manager': my_local_def_fs_manager,
              'part_assets': my_fs_part_manager.configured(config_or_config_fn={'base_dir': local_dev_folder})},
}

deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")
# deployment_name = 'staging'
resource_defs = resource_defs_by_deployment_name[deployment_name]

# Experiments with jobs from graphs:
output_nodes = [n.name for n in graph_samples.node_defs]
job_from_graph = graph_samples.to_job(resource_defs=resource_defs, config=config_mapping_factory(output_nodes))
output_nodes_multi = [n.name for n in graph_multi_sample.node_defs]
job_graph_multi = graph_multi_sample.to_job(resource_defs=resource_defs,
                                            config=config_mapping_factory(output_nodes_multi))
job_from_graph_part = partitions_graph.to_job(resource_defs=resource_defs)


nodes_for_for_asset2 = ['asset_sample2']
asset2_job = define_asset_job(name='asset2_job', selection=nodes_for_for_asset2,
                              config=config_mapping_factory(nodes_for_for_asset2))
nodes_for_for_asset12 = ['asset_sample2', 'asset_sample1', 'concat_assets']
asset12_job = define_asset_job(name='asset12_job', selection=nodes_for_for_asset12,
                               config=config_mapping_factory(nodes_for_for_asset12))

downstream_job = define_asset_job(name='downstream_job', selection=processed_asset_keys,config=downstream_conf)


dagster_poc = Definitions(
    assets=[asset_sample1, asset_sample2, concat_assets, partition_sample, partition_samplestep,
            raw_data_monthly, raw_data_scenarios, *raw_data, *proc_data
            ],
    jobs=[job_from_graph, job_graph_multi, asset12_job, asset2_job,
          job_from_graph_part,
          downstream_job,
          ],
    resources=resource_defs
)

# if __name__ == "__main__":
#
#     result = asset12_job.execute_in_process(run_config={"param_id": 0})

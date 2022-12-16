import os
import pathlib
# import sys


from dagster import repository, with_resources, define_asset_job, make_values_resource, AssetsDefinition

# from .resources.configs import output_config
from .resources import my_fs_manager
from .resources.configs import (
    output_config,
    ops_output_config,
ops_output_config_fan,
config_mapping_factory
    # globals_config
)
from .ops.ops import (
    graph_samples,
    graph_multi_sample,
    # graph_from_conf
)
# from .jobs import job_conf
from .assets.assets import (
    asset_sample1,
    asset_sample2,
concat_assets,
    # conf_asset
)


local_dev_folder = str(pathlib.Path(__file__).parents[2].joinpath('dev').resolve())
local_prod_folder = str(pathlib.Path(__file__).parents[2].joinpath('prod').resolve())
my_local_def_fs_manager = my_fs_manager.configured({'base_dir': local_dev_folder})
my_local_prod_fs_manager = my_fs_manager.configured({'base_dir': local_prod_folder})
resource_defs_by_deployment_name = {
    "prod": {'io_manager': my_local_prod_fs_manager},
    "staging": None,
    "local": {'io_manager': my_local_def_fs_manager},
}


#graph_multi_sample = AssetsDefinition.from_graph(graph_multi_sample)

@repository
def dagster_poc():
    # By default, use local
    deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")
    resource_defs = resource_defs_by_deployment_name[deployment_name]
    # job_from_graph = graph_samples.to_job(resource_defs=resource_defs, config=)

    # Works
    #job_from_graph = graph_samples.to_job(resource_defs=resource_defs, config=ops_output_config)
    output_nodes = [n.name for n in graph_samples.node_defs]
    job_from_graph = graph_samples.to_job(resource_defs=resource_defs, config=config_mapping_factory(output_nodes))

    #job_graph_multi = graph_multi_sample.to_job(resource_defs=resource_defs, config=ops_output_config_fan)

    output_nodes_multi = [n.name for n in graph_multi_sample.node_defs]
    job_graph_multi = graph_multi_sample.to_job(resource_defs=resource_defs, config=config_mapping_factory(output_nodes_multi))


    # job_conf = graph_from_conf.to_job(resource_defs={**resource_defs, **{'globals': make_values_resource(output_path=str)}})
    # job_conf2 = graph_from_conf.to_job(resource_defs={**resource_defs, **{'globals': globals_config}})

    #asset2_job = define_asset_job(name='asset2_job', selection='asset_sample2', config=output_config)
    nodes_for_for_asset2 = ['asset_sample2']
    asset2_job = define_asset_job(name='asset2_job', selection=nodes_for_for_asset2, config=config_mapping_factory(nodes_for_for_asset2))
    nodes_for_for_asset12 = ['asset_sample2','asset_sample1', 'concat_assets']
    asset12_job = define_asset_job(name='asset12_job', selection=nodes_for_for_asset12, config=config_mapping_factory(nodes_for_for_asset12))

    # asset_conf = define_asset_job(name='conf_asset_job', selection='conf_asset')
    #asset12_job = define_asset_job(name='asset12_job', selection=['asset_sample2','asset_sample1'], config=output_config)
    #nodes_multi = [n.name for n in graph_multi_sample.node_defs] + ['graph_multi_sample']
    #asset_multi_job = define_asset_job(name='multi_asset_job', selection='graph_multi_sample', config=config_mapping_factory(nodes_multi))
    return [
        # job_conf,
        job_from_graph,
        job_graph_multi,
        # job_conf2,
        asset12_job,
        asset2_job,

        #multi_assetx,
        *with_resources([asset_sample1, asset_sample2, concat_assets], resource_defs=resource_defs),
  #      asset_multi_job,
        # *with_resources([asset_sample1], resource_defs=resource_defs)
        #   all_assets_job,
    ]


# if __name__ == "__main__":
#
#     result = asset12_job.execute_in_process(run_config={"param_id": 0})

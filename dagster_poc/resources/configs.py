import os

from dagster import config_mapping, make_values_resource, List
import pathlib
import pandas as pd


def load_conf_from_csv(param_id: int) -> dict:
    df = pd.read_csv(pathlib.Path(__file__).parents[1].joinpath('run_configs.csv'), index_col=0)
    return df.loc[param_id].to_dict()


@config_mapping(config_schema={"param_id": int})
def output_config(val):
    # root_folder = local_folder
    conf = load_conf_from_csv(val["param_id"])
    # folder = get_path_from_id(val["simplified_param"])
    workspace_root = os.path.join(conf.get('run_type'), conf.get('rep_date'), conf.get('nickname'))
    ops_output_config_schema = {}
    for output in ['asset_sample1', 'asset_sample2', 'concat_samples', 'generate_sample1', 'generate_sample2']:
        output_path = os.path.join(workspace_root, 'result', f"{output}.pkl")
        ops_output_config_schema[output] = {'outputs': {'result': {'output_path': output_path}}}

    return {"ops": ops_output_config_schema}


@config_mapping(config_schema={"param_id": int})
def ops_output_config(val):
    conf = load_conf_from_csv(val["param_id"])

    workspace_root = os.path.join(conf.get('run_type'), conf.get('rep_date'), conf.get('nickname'))
    ops_output_config_schema = {}
    for output in ['concat_samples', 'generate_sample1', 'generate_single_sample']:
        output_path = os.path.join(workspace_root, 'result', f"{output}.pkl")
        ops_output_config_schema[output] = {'outputs': {'result': {'output_path': output_path}}}

    return {"ops": ops_output_config_schema}


@config_mapping(config_schema={"param_id": int})
def ops_output_config_fan(val):
    conf = load_conf_from_csv(val["param_id"])

    workspace_root = os.path.join(conf.get('run_type'), conf.get('rep_date'), conf.get('nickname'))
    ops_output_config_schema = {}
    for output in ['concat_samples']:
        output_path = os.path.join(workspace_root, 'result', f"{output}.pkl")
        ops_output_config_schema[output] = {'outputs': {'result': {'output_path': output_path}}}
        #ops_output_config_schema[output] = {'config': {'outputs': {'output_path': output_path}}}

    list_of_samples = list(range(5))
    for output_per_sample in ['generate_sample1']: #, 'generate_sample2'
        for s in list_of_samples:
            output = f"{output_per_sample}_s{s}"
            output_path = os.path.join(workspace_root, 'result', f"{output}.pkl")
            ops_output_config_schema[output] = {'outputs': {'result': {'output_path': output_path}}}

    return {"ops": ops_output_config_schema}


def config_mapping_factory(outputs: List[str]):
    @config_mapping(config_schema={"param_id": int})
    def ops_output_config_factory(val):
        conf = load_conf_from_csv(val["param_id"])
        workspace_root = os.path.join(conf.get('run_type'), conf.get('rep_date'), conf.get('nickname'))
        ops_output_config_schema = {}
        for output in outputs:
            output_path = os.path.join(workspace_root, 'result', f"{output}.pkl")
            # This works for confiured
            ops_output_config_schema[output] = {'outputs': {'result': {'output_path': output_path}}}
            # Test for alias -> alias doesn't generate nodes correctly
            #ops_output_config_schema[output] = {'config': {'outputs':{'result': {'output_path': output_path}}}}

        return {"ops": ops_output_config_schema}
    return ops_output_config_factory


#
# @config_mapping(config_schema={"param_id": int})
# def globals_config(val):
#     conf = load_conf_from_csv(val["param_id"])
#     list_of_samples = list(range(5))
#
#     workspace_root = os.path.join(conf.get('run_type'), conf.get('rep_date'), conf.get('nickname'))
#     ops_output_config_schema = {}
#     for output in ['concat_samples']: #, 'generate_sample1', 'generate_sample2']:
#         output_path = os.path.join(workspace_root, 'result', f"{output}.pkl")
#         ops_output_config_schema[output] = {'outputs': {'result': {'output_path': output_path}}}
#
#     for output_per_sample in ['generate_sample1']: #, 'generate_sample2'
#         for s in list_of_samples:
#             output = f"{output_per_sample}_s{s}"
#             output_path = os.path.join(workspace_root, 'result', f"{output}.pkl")
#             ops_output_config_schema[output] = {'outputs': {'result': {'output_path': output_path}}}
#
#     return {'config':
#                 {'run_type': conf.get('run_type'),
#                  'workspace_root': workspace_root}
#             }
#

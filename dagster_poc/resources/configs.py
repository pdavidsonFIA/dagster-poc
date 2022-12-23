import os

from dagster import config_mapping, make_values_resource, List
import pathlib
import pandas as pd


def load_conf_from_csv(param_id: int) -> dict:
    df = pd.read_csv(pathlib.Path(__file__).parents[1].joinpath('run_configs.csv'), index_col=0)
    return df.loc[param_id].to_dict()


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

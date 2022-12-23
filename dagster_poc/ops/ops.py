from typing import List
import pandas as pd

from dagster import op, graph, configured, Out

from ..common import sample_data


@op
def generate_single_sample() -> pd.DataFrame:
    # context.log.info("config_param: " + context.op_config["config_param"])
    return sample_data()


@op
def generate_sample1() -> pd.DataFrame:
    # context.log.info("config_param: " + context.op_config["config_param"])
    return sample_data()


@op
def concat_samples(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(dfs, axis=0)
    df = df.groupby(['mgroup_id', 'd_from']).sum()
    return df

@op
def concat_only(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(dfs, axis=0)
    # df = df.groupby(['mgroup_id', 'd_from']).sum()
    return df


@graph()
def graph_samples():
    df1 = generate_sample1()
    df2 = generate_single_sample()
    return concat_samples([df1, df2])


@graph
def graph_stacked():
    df1 = graph_samples()
    df2 = graph_samples()
    return concat_samples([df1, df2])


@op
def summarize_data(df) -> pd.DataFrame:
    return df.groupby(['mgroup_id', 'd_from']).sum()


@graph
def graph_multi_sample():
    n_samples = 5
    samples = []
    for i in range(n_samples):
        sample = configured(generate_sample1, name=f"generate_sample1_s{i}")({})()
        samples.append(sample)
        #samples.append(generate_sample1.alias(f'generate_sample1_s{i}')())

    # asset1 = dagster_poc.get_asset_value_loader().load_asset_value('asset_sample1')
    # samples.append(asset1)
    return concat_samples(samples)


@op()
def load_pickle(key):
    fpath = f'C:\\Users\peter.davidson\PycharmProjects\dev\partition_sample\\{key}'
    return pd.read_pickle(fpath)


from ..partitions import all_scenarios
# def partition_loader_factory():
#     keys = all_scenarios.get_partition_keys()
#     @op(out={f'partition_sample_{key}': Out() for key in keys})
#     def asset_loader():
#         return tuple(configured(load_pickle, name=key)({})(key) for key in keys)
#     return asset_loader()


keys = all_scenarios.get_partition_keys()
@op(out={f'partition_sample_{key}': Out() for key in keys})
def asset_loader():
    return tuple(configured(load_pickle, name=key)({})(key) for key in keys)



@graph
def partitions_graph():
    samples = []
    for i in list(asset_loader()):
        sample = i
        samples.append(sample)
    return concat_samples(samples)



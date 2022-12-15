from datetime import date
from typing import List
import pandas as pd

from dagster import op, graph, configured


def sample_data() -> pd.DataFrame:
    df = {
        1: {'mgroup_id': 1, 'd_from': date(2020, 1, 1), 'income':100},
        2: {'mgroup_id': 2, 'd_from': date(2020, 1, 1), 'income':100},
        3: {'mgroup_id': 3, 'd_from': date(2020, 1, 1), 'income':100},
        4: {'mgroup_id': 4, 'd_from': date(2020, 1, 1), 'income':100},
    }

    df = pd.DataFrame.from_dict(df, orient='index')

    df = df.astype({'d_from': 'datetime64[ns]',  'income': 'float64'})
    return df

@op
def generate_single_sample() -> pd.DataFrame:
    # context.log.info("config_param: " + context.op_config["config_param"])
    return sample_data()


# @op(config_schema={'sample_id': int})
# def generate_conf_sample() -> pd.DataFrame:
#     # context.log.info("config_param: " + context.op_config["config_param"])
#     return sample_data()


# @graph
# def graph_from_conf():
#     generate_conf_sample()



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
    return concat_samples(samples)

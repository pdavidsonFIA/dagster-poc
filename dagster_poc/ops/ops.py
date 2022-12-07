from datetime import date
from typing import List

import pandas as pd
from dagster import op, graph


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
def generate_sample1() -> pd.DataFrame:
    # context.log.info("config_param: " + context.op_config["config_param"])
    return sample_data()

@op
def generate_sample2() -> pd.DataFrame:
    # context.log.info("config_param: " + context.op_config["config_param"])
    return sample_data()


@op
def concat_samples(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(dfs, axis=0)
    df = df.groupby(['mgroup_id', 'd_from']).sum()
    return df


@graph()
def graph_samples():
    df1 = generate_sample1()
    df2 = generate_sample2()
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
    n_samples = 10
    samples = []
    for i in range(n_samples):
        samples.append(summarize_data.alias(f"summary_{i}")(generate_sample1.alias(f"sample_{i}")()))
    return concat_samples(samples)

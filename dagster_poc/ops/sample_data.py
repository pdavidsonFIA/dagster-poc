from datetime import date

import pandas as pd
from dagster import op, In


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


# @op(config_schema={"config_param": str},
#     out={'test': Out()},
#     )
@op
def generate_sample1() -> pd.DataFrame:
    # context.log.info("config_param: " + context.op_config["config_param"])
    return sample_data()

@op
def generate_sample2() -> pd.DataFrame:
    # context.log.info("config_param: " + context.op_config["config_param"])
    return sample_data()


@op
def aggregate_samples() -> pd.DataFrame:
    df = pd.concat((generate_sample1(), generate_sample2()), axis=0)
    df = df.groupby(['mgroup_id', 'd_from']).sum()
    return df

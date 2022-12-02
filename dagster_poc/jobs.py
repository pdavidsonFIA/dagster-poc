import pandas as pd
from dagster import job, op

from .ops.sample_data import generate_sample1, generate_sample2, aggregate_samples
from .resources import my_io_manager
from .resources.simple_config import simplified_config


@job(config=simplified_config,
     resource_defs={'io_manager': my_io_manager}
     )
def aggregate_job():
#     generate_aggregated_samples()
#
#
# @op
# # def aggregate_samples(context, df1, df2) -> pd.DataFrame:
# #     context.log.info("Samples aggregation: " + context.op_config["config_param"])
# def generate_aggregated_samples() -> pd.DataFrame:
#     df1 = generate_sample1()
#     df2 = generate_sample2()
#     aggregate_samples(df1, df2)
    aggregate_samples()
    # return df

# import os
#
# from dagster import get_dagster_logger, job, op
#
#
# @op
# def get_file_sizes():
#     files = [f for f in os.listdir(".") if os.path.isfile(f)]
#     return {f: os.path.getsize(f) for f in files}
#
#
# @op
# def get_total_size(file_sizes):
#     return sum(file_sizes.values())
#
#
# @op
# def get_largest_size(file_sizes):
#     return max(file_sizes.values())
#
#
# @op
# def report_file_stats(total_size, largest_size):
#     # In real life, we'd send an email or Slack message instead of just logging:
#     get_dagster_logger().info(f"Total size: {total_size}, largest size: {largest_size}")
#
#
# @job(config=simplified_config,
#             resource_defs={'io_manager': my_io_manager})
# def diamond():
#     file_sizes = get_file_sizes()
#     report_file_stats(
#         total_size=get_total_size(file_sizes),
#         largest_size=get_largest_size(file_sizes),
#     )
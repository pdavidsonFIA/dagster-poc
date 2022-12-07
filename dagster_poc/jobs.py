from dagster import job
from .ops.ops import (
    generate_sample1,
    generate_sample2,
    concat_samples,
    graph_samples,
    graph_stacked
)
from .resources import my_io_manager_int

@job(resource_defs={'io_manager': my_io_manager_int})
def job_int_param():
    df1 = generate_sample1()
    df2 = generate_sample2()
    concat_samples(df1, df2)


job_from_graph = graph_samples.to_job(resource_defs={'io_manager': my_io_manager_int})
job_from_graph_stacked = graph_stacked.to_job(resource_defs={'io_manager': my_io_manager_int})
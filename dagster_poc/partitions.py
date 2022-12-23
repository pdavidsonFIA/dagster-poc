from dagster import (
    StaticPartitionsDefinition,
    MonthlyPartitionsDefinition,
)
all_months = MonthlyPartitionsDefinition(start_date='2022-01-01')
all_scenarios = StaticPartitionsDefinition('abc')

# Param sets numbered 0, 1, 2 -> note partition key is a string
all_params = StaticPartitionsDefinition(''.join([str(i) for i in range(3)]))
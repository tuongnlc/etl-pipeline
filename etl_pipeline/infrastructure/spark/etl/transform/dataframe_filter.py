from functools import reduce

from etl_pipeline.templates.etl.transform.base import TransformStep
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col


class DataFrameFilter(TransformStep):
    """
        Filter DataFrame by conditions defined in the config file.

        Config example:
            transform_steps:
              - kind: etl_pipeline.infrastructure.spark.etl.transform.dataframe_filter.DataFrameFilter
                spec:
                  condition:
                    - date: "2024-01-01"
                    - url: "https://example.com"
    """
    def __init__(self, condition: list[dict]) -> None:
        self.condition = condition

    def transform(self, df: DataFrame) -> DataFrame:
        """
            Build the filter expressions from the condition list and apply
            them to the DataFrame (conditions are combined with AND).
        """
        conditions: list[Column] = [
            col(column) == value
            for item in self.condition or []
            for column, value in item.items()
            if value is not None and value != ""
        ]

        if not conditions:
            return df

        return df.filter(reduce(lambda left, right: left & right, conditions))
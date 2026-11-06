from etl_pipeline.templates.etl.transform.base import TransformStep
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from typing import Any


class AddDateColumn(TransformStep):
    """
        AddDateColumn with column_type is date
    """
    def __init__(self, column_name: str, value: Any) -> None:
        self.column_name = column_name
        self.value = value

    def transform(self, df: DataFrame) -> DataFrame:
        """
            Add date column to df
        """
        return df.withColumn(self.column_name, lit(self.value))
        
class SelectColumns(TransformStep):
    """
        SelectColumns with column_name list
    """
    def __init__(self, column_names: list) -> None:
        self.column_names = column_names

    def transform(self, df: DataFrame) -> DataFrame:
        """
            Set column value to df
        """
        return df.select(*self.column_names)


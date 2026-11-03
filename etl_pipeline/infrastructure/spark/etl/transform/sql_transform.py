from pyspark.sql import SparkSession, DataFrame

from etl_pipeline.templates.etl.transform.base import BaseTransform, TransformStep


class SqlTransformWithSpark(BaseTransform):
    """
        Transform a Spark DataFrame by registering it as a temporary view
        then running a SQL query against that view.
    """
    def __init__(
        self,
        spark: SparkSession,
        sql_query: str,
    ):
        self.spark = spark
        self.sql_query = sql_query

    def transform(self, transform_steps: list[TransformStep] = None) -> DataFrame:
        """
            Register the input DataFrame as a temp view then transform it using the SQL query
        """
        return self.spark.sql(self.sql_query)
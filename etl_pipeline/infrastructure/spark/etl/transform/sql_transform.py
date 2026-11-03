from pyspark.sql import SparkSession, DataFrame



class SqlTransformWithSpark:
    def __init__(self, spark: SparkSession, sql_query: str):
        self.spark = spark
        self.sql_query = sql_query

    def transform(self) -> DataFrame:
        """
            Transform data using SQL query
        """
        return self.spark.sql(self.sql_query)
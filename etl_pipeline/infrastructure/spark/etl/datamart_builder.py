from pyspark.sql import SparkSession, DataFrame


class DatamartBuilder:
    """
        Build datamart bằng cách chạy câu SQL (CTAS/INSERT...) trên Spark.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build(self, etl_sql: str) -> DataFrame:
        """
            Chạy câu SQL để build datamart.
        """
        return self.spark.sql(etl_sql)

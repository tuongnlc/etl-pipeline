import pyspark.sql as ss
from pyspark.sql import SparkSession


class ParquetExtractor():
    def extract(
        self,
        parquet_path: str,
        spark: SparkSession,
    ):
        """
            Extract data from parquet file
        """
        df = spark.read.parquet(parquet_path)
        return df
        
from pyspark.sql import SparkSession
from etl_pipeline.templates.etl.extract.parquet_extractpr import ParquetExtractor


class ParquetExtractorWithSpark(ParquetExtractor):
    def __init__(
        self,
        parquet_uri: str,
        spark: SparkSession,
    ):
        self.parquet_uri = parquet_uri
        self.spark = spark

    def extract(self):
        """
            Extract data from parquet file
        """
        return self.spark.read.parquet(self.parquet_uri)

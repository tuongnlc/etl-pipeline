from pyspark.sql import SparkSession
from etl_pipeline.templates.etl.extract.delta_extractor import DeltaExtractor


class DeltaExtractorWithSpark(DeltaExtractor):
    def __init__(
        self,
        table_uri: str,
        spark: SparkSession,
    ):
        self.table_uri = table_uri
        self.spark = spark

    def extract(self):
        """
            Extract data from parquet file
        """
        return self.spark.read.table(self.table_uri)

from etl_pipeline.templates.etl.extract.parquet_extractpr import ParquetExtractor
from etl_pipeline.templates.etl.load.delta_loader import DeltaLoader
from pyspark.sql import DataFrame



class SilverDatabrickLakehouseJob:
    """
        Pipeline to build DataLake on Databricks

        The pipeline including:
        - Extract data from Databricks Volume
        - Load data to Databricks Catalog
    """
    def __init__(
        self, 
        extractor: ParquetExtractor,
        loader: DeltaLoader,
    ):
        self.extractor = extractor
        self.loader = loader

    def extract(self):
        data_from_databricks = self.extractor.extract()
        return data_from_databricks

    def load(self, df: DataFrame):
        """
            Load data to delta table
        """
        self.loader.load(df)

    def run(self) -> None:
        data_ = self.extract()
        self.load(data_)
        
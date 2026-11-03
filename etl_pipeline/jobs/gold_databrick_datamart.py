from etl_pipeline.templates.etl.extract.parquet_extractpr import ParquetExtractor
from etl_pipeline.templates.etl.load.delta_loader import DeltaLoader
from etl_pipeline.templates.etl.transform.base import BaseTransform
from etl_pipeline.templates.etl.transform.base import TransformStep
from pyspark.sql import DataFrame


class GoldDatabrickDatamartJob:
    """
        Pipeline to build DataLake on Databricks

        The pipeline including:
        - Extract data from Databricks Volume
        - Transform data
        - Load data to Databricks Catalog
    """
    def __init__(
        self, 
        extractor: ParquetExtractor,
        transform: BaseTransform,
        transform_steps: list[TransformStep],
        loader: DeltaLoader,
    ):
        self.extractor = extractor
        self.transformer = transform
        self.transform_steps = transform_steps or []
        self.loader = loader

    def extract(self):
        data_from_databricks = self.extractor.extract()
        return data_from_databricks

    def transform(self, data: DataFrame) -> DataFrame:
        """
            Transform data using transform_steps
        """
        return self.transformer.transform(data, self.transform_steps)

    def load(self, df: DataFrame):
        """
            Load data to delta table
        """
        self.loader.load(df)

    def run(self) -> None:
        data_ = self.extract()
        data_ = self.transform(data_)
        self.load(data_)
        
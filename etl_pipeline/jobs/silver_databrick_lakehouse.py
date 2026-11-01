import polars as pl
from etl_pipeline.templates.etl.extract.parquet_extractpr import ParquetExtractor
from etl_pipeline.templates.etl.transform.base import BaseTransform
from etl_pipeline.templates.etl.transform.base import TransformStep


class SilverDatabrickLakehouseJob:
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
        # loader: QdrantLoader,
    ):
        self.extractor = extractor
        self.transformer = transform
        self.transform_steps = transform_steps or []

    def extract(self):
        data_from_databricks = self.extractor.extract()
        return data_from_databricks
        
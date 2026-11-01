from etl_pipeline.templates.pipeline.base import BasePipeline
from etl_pipeline.templates.etl.extract.base import BaseExtractor
import polars as pl
from etl_pipeline.templates.etl.transform.base import BaseTransform
from etl_pipeline.templates.etl.transform.base import TransformStep
from etl_pipeline.templates.etl.load.qdrant_loader import QdrantLoader



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
        extractor: BaseExtractor,
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
        
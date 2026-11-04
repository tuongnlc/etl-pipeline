# market_data_etl.pyfrom etl_pipeline.templates.pipeline.base import BasePipeline
from etl_pipeline.templates.etl.load.databricks_volume_loader import DatabricksVolumeLoader
from etl_pipeline.templates.pipeline.base import BasePipeline
import polars as pl
from etl_pipeline.templates.etl.transform.base import BaseTransform, TransformStep
from etl_pipeline.templates.etl.extract.qdrant_extractor import QdrantExtractor


class BronzeQdrantDatabricks(BasePipeline):
    """
        BronzeQdrantDatabricks pipeline
    """
    def __init__(
            self, 
            extractor: QdrantExtractor,
            transformer: BaseTransform,
            transform_steps: list[TransformStep],
            loader: DatabricksVolumeLoader,
        ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.transform_steps = transform_steps
        self.loader = loader

    def extract(self) -> None:
        """
            Read data from qdrant database
        """
        data_from_qdrant = self.extractor.extract()
        return data_from_qdrant
        
    def transform(self, data: pl.DataFrame) -> pl.DataFrame:
        """
            Transform data using transform_steps
        """
        df = self.transformer.transform(data, self.transform_steps)
        if isinstance(df, pl.DataFrame):
            return df

    def load(self, transform_data: pl.DataFrame) -> None:
        """
            Load data to bigquery
        """
        self.loader.load(transform_data)

    def run(self) -> None:
        data_ = self.extract()
        data_ = self.transform(data_)
        self.load(data_)
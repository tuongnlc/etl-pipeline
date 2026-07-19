from etl_pipeline.templates.pipeline.base import BasePipeline
from etl_pipeline.templates.etl.extract.base import BaseExtractor
import polars as pl
from etl_pipeline.templates.etl.transform.base import BaseTransform
from etl_pipeline.templates.etl.transform.base import TransformStep
from etl_pipeline.templates.etl.load.qdrant_loader import QdrantLoader


class SilverQdrantEmbedding(BasePipeline):
    """
        SilverQdrantEmbedding pipeline

        The pipeline including:
        - Extract data from Qdrant
        - Transform data focusing on embedd data
        - Load data to Qdrant
    """
    def __init__(
            self, 
            extractor: BaseExtractor,
            transform: BaseTransform,
            transform_steps: list[TransformStep],
            loader: QdrantLoader,
        ) -> None:
        self.extractor = extractor
        self.transformer = transform
        self.transform_steps = transform_steps or []
        self.loader = loader

    def extract(self):
        data_from_qdrant = self.extractor.extract()
        return data_from_qdrant
        
    def transform(self, df: pl.DataFrame, transform_steps: list[TransformStep]):
        df = self.transformer.transform(df, transform_steps)
        return df
    
    def load(self, df: pl.DataFrame):
        self.loader.load(
            df, 
            dense_vector_column="dense_vector_embedded", 
            sparse_vector_indices_column="sparse_vector_indices", 
            sparse_vector_values_column="sparse_vector_value"
        )
    
    def run(self) -> None:
        data_ = self.extract()
        data_ = data_.limit(200)
        print("Number of records to transform:", len(data_))
        df = self.transform(data_, self.transform_steps)
        self.load(df)
        return df

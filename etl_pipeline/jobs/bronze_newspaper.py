from etl_pipeline.templates.etl.extract.postgre_db import PostgreDBExtractor
from etl_pipeline.templates.etl.load.qdrant_loader import QdrantLoader
from etl_pipeline.templates.pipeline.base import BasePipeline
import polars as pl
from etl_pipeline.templates.etl.transform.base import TransformStep, BaseTransform


class BronzeNewspaper(BasePipeline):
    """
        Pipeline to do ETL newspaper data from Postgres to Qdrant.
    """

    def __init__(self, 
            extractor: PostgreDBExtractor, 
            transform: BaseTransform,
            transform_steps: list[TransformStep],
            loader: QdrantLoader
        ):
        self.extractor = extractor
        self.transformer = transform
        self.transform_steps = transform_steps
        self.loader = loader
    
    def extract(self):
        data_from_postgres = self.extractor.extract()
        return data_from_postgres

    def transform(self, df: pl.DataFrame, transform_steps: list[TransformStep]):
        df = self.transformer.transform(df, transform_steps)
        if isinstance(df, pl.DataFrame):
            return df

    def load(self, transformed_data):
        self.loader.load(transformed_data, dense_vector_column=None)

    def run(self) -> None:
        data_ = self.extract()
        df = self.transform(data_, self.transform_steps)
        self.load(df)        

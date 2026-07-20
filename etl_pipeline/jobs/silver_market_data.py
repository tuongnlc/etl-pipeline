# market_data_etl.pyfrom etl_pipeline.templates.pipeline.base import BasePipeline
from etl_pipeline.templates.etl.extract.postgre_db import PostgreDBExtractor
from etl_pipeline.templates.etl.load.bq_loader import BigQueryLoader
from etl_pipeline.templates.pipeline.base import BasePipeline
import polars as pl
from etl_pipeline.templates.etl.transform.base import BaseTransform, TransformStep



class SilverMarketData(BasePipeline):
    """
        SilverMarketData pipeline
    """
    def __init__(
            self, 
            extractor: PostgreDBExtractor,
            transformer: BaseTransform,
            transform_steps: list[TransformStep],
            loader: BigQueryLoader,
        ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.transform_steps = transform_steps
        self.loader = loader

    def extract(self) -> None:
        """
            Read data from postgresql database
        """
        data_from_postgresql = self.extractor.extract()
        return data_from_postgresql
        
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
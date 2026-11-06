from etl_pipeline.infrastructure.spark.etl.load.delta_loader import DeltaLoaderWithSpark
from etl_pipeline.templates.etl.extract.parquet_extractpr import ParquetExtractor
from etl_pipeline.templates.pipeline.base import BasePipeline
import polars as pl
from etl_pipeline.templates.etl.transform.base import BaseTransform, TransformStep


class SilverCrawlNewspaper(BasePipeline):
    """
        SilverCrawlNewspaper pipeline
    """
    def __init__(
            self, 
            extractor: ParquetExtractor,
            transformer: BaseTransform,
            transform_steps: list[TransformStep],
            loader: DeltaLoaderWithSpark,
        ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.transform_steps = transform_steps
        self.loader = DeltaLoaderWithSpark

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
            Load data to databricks volume
        """
        self.loader.load(transform_data)

    def run(self) -> None:
        data_ = self.extract()
        data_ = self.transform(data_)
        self.load(data_)
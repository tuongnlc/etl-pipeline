from etl_pipeline.infrastructure.crawler.crawl_newspaper.crawl_newspaper import CrawlNewspaper
from etl_pipeline.infrastructure.spark.etl.load.delta_loader import DeltaLoaderWithSpark
from etl_pipeline.templates.etl.extract.delta_extractor import DeltaExtractor
from etl_pipeline.templates.pipeline.base import BaseCrawlerPipeline
from etl_pipeline.templates.etl.transform.base import BaseTransform, TransformStep
from pyspark.sql import DataFrame

class SilverCrawlNewspaper(BaseCrawlerPipeline):
    """
        SilverCrawlNewspaper pipeline
    """
    def __init__(
            self, 
            extractor: DeltaExtractor,
            transformer: BaseTransform,
            transform_steps: list[TransformStep],
            crawler: CrawlNewspaper,
            loader: DeltaLoaderWithSpark,
        ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.transform_steps = transform_steps
        self.crawler = crawler
        self.loader = loader

    def extract(self) -> DataFrame:
        """
            Read data from datalake
        """
        data_from_datalake = self.extractor.extract()
        return data_from_datalake
        
    def transform(self, df: DataFrame) -> DataFrame:
        """
            Transform data using transform_steps
        """
        df = self.transformer.transform(df, self.transform_steps)
        return df

    def crawl(self, df: DataFrame) -> None:
        """
            Crawl data 
        """
        df = self.crawler.crawl(df)
        return df

    def load(self, df: DataFrame) -> None:
        """
            Load data to databricks volume
        """
        self.loader.load(df)
        return df

    def run(self) -> None:
        data_ = self.extract()
        data_ = self.transform(data_)
        data_ = self.crawl(data_)
        self.load(data_)
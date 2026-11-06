from etl_pipeline.infrastructure.crawler.crawl_newspaper.crawl_newspaper_url import CrawlNewspaperUrl
from etl_pipeline.infrastructure.spark.etl.load.delta_loader import DeltaMergeLoaderWithSpark
from etl_pipeline.templates.pipeline.base import BaseCrawlerPipeline
from pyspark.sql import DataFrame
from etl_pipeline.templates.etl.transform.base import TransformStep


class BronzeCrawlNewspaperUrl(BaseCrawlerPipeline):
    """
        BronzeCrawlNewspaperUrl pipeline
    """
    def __init__(
            self, 
            crawler: CrawlNewspaperUrl,
            transform_steps: list[TransformStep],
            loader: DeltaMergeLoaderWithSpark,
        ) -> None:
        self.crawler = crawler
        self.transform_steps = transform_steps
        self.loader = loader

    def crawl(self) -> None:
        """
            Read data from postgresql database
        """
        data_crawler = self.crawler.crawl()
        return data_crawler
        
    def transform(self, data: DataFrame) -> DataFrame:
        """
            Transform data using transform_steps
        """
        for step in self.transform_steps:
            data = step.transform(data)
        return data

    def load(self, data: DataFrame) -> None:
        """
            Load data to databricks volume
        """
        self.loader.load(data)

    def run(self) -> None:
        data_ = self.crawl()
        data_ = self.transform(data_)
        self.load(data_)

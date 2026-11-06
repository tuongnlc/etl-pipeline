# market_data_etl.pyfrom etl_pipeline.templates.pipeline.base import BasePipeline
from etl_pipeline.infrastructure.crawler.extract.crawl_newspaper_url import CrawlNewspaperUrl
from etl_pipeline.infrastructure.spark.etl.load.delta_loader import DeltaMergeLoaderWithSpark
from etl_pipeline.templates.pipeline.base import BasePipeline
from pyspark.sql import DataFrame
from etl_pipeline.templates.etl.transform.base import TransformStep

class BronzeCrawlNewspaperUrl(BasePipeline):
    """
        BronzeCrawlNewspaperUrl pipeline
    """
    def __init__(
            self, 
            extractor: CrawlNewspaperUrl,
            transform_steps: list[TransformStep],
            loader: DeltaMergeLoaderWithSpark,
        ) -> None:
        self.extractor = extractor
        self.transform_steps = transform_steps
        self.loader = loader

    def extract(self) -> None:
        """
            Read data from postgresql database
        """
        data_crawler = self.extractor.extract()
        print(data_crawler)
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
        data_ = self.extract()
        data_ = self.transform(data_)
        self.load(data_)

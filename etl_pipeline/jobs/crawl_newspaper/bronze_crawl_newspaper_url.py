# market_data_etl.pyfrom etl_pipeline.templates.pipeline.base import BasePipeline
from etl_pipeline.infrastructure.crawler.extract.crawl_newspaper_url import CrawlNewspaperUrl
from etl_pipeline.templates.etl.load.databricks_volume_loader import DatabricksVolumeLoader
from etl_pipeline.templates.pipeline.base import BasePipeline
from etl_pipeline.templates.etl.transform.base import BaseTransform, TransformStep


class BronzeCrawlNewspaperUrl(BasePipeline):
    """
        BronzeCrawlNewspaperUrl pipeline
    """
    def __init__(
            self, 
            extractor: CrawlNewspaperUrl,
            # transformer: BaseTransform,
            # transform_steps: list[TransformStep],
            # loader: DatabricksVolumeLoader,
        ) -> None:
        self.extractor = extractor
        # self.transformer = transformer
        # self.transform_steps = transform_steps
        # self.loader = loader

    def extract(self) -> None:
        """
            Read data from postgresql database
        """
        data_crawler = self.extractor.extract()
        print(data_crawler)
        return data_crawler
        
    # def transform(self, data: pl.DataFrame) -> pl.DataFrame:
    #     """
    #         Transform data using transform_steps
    #     """
    #     df = self.transformer.transform(data, self.transform_steps)
    #     if isinstance(df, pl.DataFrame):
    #         return df

    def load(self) -> None:
        """
            Load data to databricks volume
        """
        return None

    def run(self) -> None:
        data_ = self.extract()
        # data_ = self.transform(data_)
        # self.load(data_)
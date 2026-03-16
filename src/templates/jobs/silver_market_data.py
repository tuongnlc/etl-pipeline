from templates.pipeline.base import BasePipeline
from templates.etl.extract.postgre_db import PostgreDBExtractor
from templates.etl.load.bq_loader import BigQueryLoader



class SilverMarketData(BasePipeline):
    """
        SilverMarketData pipeline
    """
    def __init__(
            self, 
            extractor: PostgreDBExtractor,
            # transformer: BaseTransformer,
            loader: BigQueryLoader,
        ) -> None:
        self.extractor = extractor
        # self.transformer = transformer
        self.loader = loader

    def extract(self) -> None:
        """
            Read data from postgresql database
        """
        data_from_postgresql = self.extractor.extract()
        return data_from_postgresql
        
    def transform(self) -> None:
        pass

    def load(self, transform_data) -> None:
        """
            Load data to bigquery
        """
        self.loader.load(transform_data)

    def run(self) -> None:
        data_ = self.extract()
        # self.transform(data_)
        self.load(data_)

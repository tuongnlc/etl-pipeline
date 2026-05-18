from src.templates.etl.extract.postgre_db import PostgreDBExtractor
from src.templates.etl.load.qdrant_loader import QdrantLoader
from src.templates.pipeline.base import BasePipeline


class BronzeNewspaper(BasePipeline):
    """
        Pipeline to do ETL newspaper data from Postgres to Qdrant.
    """

    def __init__(self, 
            extractor: PostgreDBExtractor, 
            loader: QdrantLoader
        ):
        super().__init__(extractor, loader)
        self.extractor = extractor
        self.loader = loader
    
    def extract(self):
        data_from_postgres = self.extractor.extract()
        return data_from_postgres

    def transform():
        pass

    def load(self, transformed_data):
        self.loader.load(transformed_data)

    def run(self) -> None:
        data_ = self.extract()
        self.load(data_)

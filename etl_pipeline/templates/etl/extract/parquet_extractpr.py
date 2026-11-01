#interface to read parquet data 
from etl_pipeline.templates.etl.extract.base import BaseExtractor

class ParquetExtractor(BaseExtractor):
    def extract(self) -> None:
        raise NotImplementedError("extract method must be implemented")

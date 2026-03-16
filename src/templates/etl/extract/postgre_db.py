#interface to read data from postgresql database
from src.templates.etl.extract.base import BaseExtractor

class PostgreDBExtractor(BaseExtractor):
    def extract(self, query: str, uri: str, **kwargs) -> None:
        raise NotImplementedError("extract method must be implemented")
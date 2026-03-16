#interface to read data from postgresql database
from templates.etl.extract.base import BaseExtractor

class PostgreDBExtractor(BaseExtractor):
    def extract(self) -> None:
        raise NotImplementedError("extract method must be implemented")
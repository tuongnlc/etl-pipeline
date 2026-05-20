from src.templates.etl.extract.base import BaseExtractor

class QdrantExtractor(BaseExtractor):
    def extract(self) -> None:
        raise NotImplementedError("extract method must be implemented")

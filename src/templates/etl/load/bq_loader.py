from src.templates.etl.load.base import BaseLoader

class BigQueryLoader(BaseLoader):
    def load(self) -> None:
        raise NotImplementedError("load method must be implemented")
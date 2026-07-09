from etl_pipeline.templates.etl.load.base import BaseLoader
import polars as pl

class QdrantLoader(BaseLoader):
    def load(self, df: pl.DataFrame) -> None:
        raise NotImplementedError("load method must be implemented")
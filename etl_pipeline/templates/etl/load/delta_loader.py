from etl_pipeline.templates.etl.load.base import BaseLoader
from pyspark.sql import DataFrame


class DeltaLoader(BaseLoader):
    def load(self, df: DataFrame) -> None:
        raise NotImplementedError("load method must be implemented")
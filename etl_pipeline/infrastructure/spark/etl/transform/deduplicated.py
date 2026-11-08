from etl_pipeline.templates.etl.transform.base import TransformStep
from pyspark.sql import DataFrame


class DeduplicatedRows(TransformStep):
    """
        DeduplicatedRows with deduplicate_columns list
    """
    def __init__(self, deduplicate_columns: list = None) -> None:
        self.deduplicate_columns = deduplicate_columns

    def transform(self, df: DataFrame) -> DataFrame:
        """
            Add date column to df
        """
        return df.drop_duplicates(subset=self.deduplicate_columns)

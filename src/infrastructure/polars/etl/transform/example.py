import polars as pl
from src.templates.etl.transform.base import TransformStep


class ExampleTransformStep(TransformStep):
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
            Example transform step.
        """
        print("Hello Transform")
        return df

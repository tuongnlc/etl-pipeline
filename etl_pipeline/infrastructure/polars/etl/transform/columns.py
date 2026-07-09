import polars as pl
from etl_pipeline.templates.etl.transform.base import TransformStep
from typing import Any



class SelectColumns(TransformStep):
    def __init__(self, columns: list[str]):
        self.columns = columns

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df= df.select(pl.col(c) for c in self.columns)
        return df


class AddColumn(TransformStep):
    def __init__(self, column: str, value: Any):
        self.column = column
        self.value = value

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.lit(self.value).alias(self.column))


class RemoveColumn(TransformStep):
    def __init__(self, column: str):
        self.column = column

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.drop(self.column)

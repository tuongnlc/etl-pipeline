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


class RenameColumns(TransformStep):
    def __init__(self, column_mappings: dict):
        self.column_mappings =  column_mappings

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.rename(self.column_mappings)


class ConcatColumns(TransformStep):
    def __init__(self, concat_columns: list, new_column: str):
        self.concat_columns = concat_columns
        self.new_column = new_column

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        # return super().transform(df)
        exprs = []
        print(self.concat_columns)
        for col in self.concat_columns:
            # print(col)
            if df[col].dtype in [pl.Date, pl.Datetime]:
                formatted_col = pl.col(col).dt.strftime("%Y-%m-%d")
            else:
                formatted_col = pl.col(col).cast(pl.String)
                
            exprs.append(pl.format("{}: {}", pl.lit(col), formatted_col))

        df = df.with_columns(
            pl.concat_str(*exprs, separator=", ").alias(self.new_column),
        )
        return df
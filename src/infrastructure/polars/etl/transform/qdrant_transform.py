from src.templates.etl.transform.base import BaseTransform, TransformStep
import polars as pl


class QdrantTransform(BaseTransform):
    def transform(self, df: pl.DataFrame, transform_steps: list[TransformStep]) -> pl.DataFrame:
        for step in transform_steps:
            df = step.transform(df)
        print(df)
        return df
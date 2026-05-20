from abc import ABC, abstractmethod
import polars as pl


class TransformStep(ABC):
    @abstractmethod
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
            Transform the input DataFrame.
        """
        raise NotImplementedError("transform method must be implemented")


class BaseTransform:
    def transform(self, df: pl.DataFrame, transform_steps: list[TransformStep]) -> pl.DataFrame:
        for step in transform_steps:
            df = step.transform(df)
        return df

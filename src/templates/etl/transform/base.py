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
    @abstractmethod
    def transform(self, df: pl.DataFrame, transform_steps: list[TransformStep]) -> pl.DataFrame:
        raise NotImplementedError("transform method must be implemented")
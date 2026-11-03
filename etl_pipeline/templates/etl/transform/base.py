from abc import ABC, abstractmethod


class TransformStep(ABC):
    @abstractmethod
    def transform(self, df):
        """
            Transform the input DataFrame.
        """
        raise NotImplementedError("transform method must be implemented")


class BaseTransform:
    @abstractmethod
    def transform(self, df, transform_steps: list[TransformStep]):
        raise NotImplementedError("transform method must be implemented")
from abc import ABC, abstractmethod
from typing import Any



class BasePipeline(ABC):
    @abstractmethod
    def extract(self) -> None:
        raise NotImplementedError("extract method must be implemented")

    def transform(selfs, df, transformation_steps: list[Any]) -> None:
        """
            Transform the extracted data.
        """
        if transformation_steps is not None:
            for step in transformation_steps:
                df = step.transform(df)
        if df is not None:
            return df
        return None

    @abstractmethod
    def load(self) -> None:
        raise NotImplementedError("load method must be implemented")
        
    def run(self, df, transformation_steps: list[Any]) -> None:
        self.extract()
        df = self.transform(df, transformation_steps)
        self.load()

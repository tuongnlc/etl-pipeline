from abc import ABC, abstractmethod


class BasePipeline(ABC):
    @abstractmethod
    def extract(self) -> None:
        raise NotImplementedError("extract method must be implemented")

    def transform(self) -> None:
        pass

    @abstractmethod
    def load(self) -> None:
        raise NotImplementedError("load method must be implemented")

    def run(self) -> None:
        self.extract()
        self.transform()
        self.load()

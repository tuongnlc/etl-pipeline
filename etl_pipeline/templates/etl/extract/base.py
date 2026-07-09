from abc import ABC, abstractmethod

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self) -> None:
        raise NotImplementedError("extract method must be implemented")
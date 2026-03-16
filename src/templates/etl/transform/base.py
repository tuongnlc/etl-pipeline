from abc import ABC, abstractmethod

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self) -> None:
        raise NotImplementedError("transform method must be implemented")
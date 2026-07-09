
from abc import abstractmethod
from templates.pipeline.base import BasePipeline

class MarketDataPipeline(BasePipeline):
    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError("run method must be implemented")
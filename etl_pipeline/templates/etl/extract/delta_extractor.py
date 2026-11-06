from etl_pipeline.templates.etl.extract.base import BaseExtractor


class DeltaExtractor(BaseExtractor):
    def extract(self) -> None:
        raise NotImplementedError("extract method must be implemented")

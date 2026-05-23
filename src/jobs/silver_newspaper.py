from src.templates.pipeline.base import BasePipeline
from src.templates.etl.extract.base import BaseExtractor
import polars as pl
from src.templates.etl.transform.base import BaseTransform
from src.templates.etl.transform.base import TransformStep


class SilverNewspaper(BasePipeline):
    """
        SilverNewspaper pipeline
    """
    def __init__(
            self, 
            extractor: BaseExtractor,
            transform: BaseTransform,
            transform_steps: list[TransformStep],
            # loader: BaseLoader,
        ) -> None:
        self.extractor = extractor
        self.transformer = transform
        self.transform_steps = transform_steps or []
        # self.loader = loader

    def extract(self):
        data_from_qdrant = self.extractor.extract()
        return data_from_qdrant
        
    def transform(self, df: pl.DataFrame, transform_steps: list[TransformStep]):
        df = self.transformer.transform(df, transform_steps)
        print(df)
        return df
    
    def load(self, transformed_data):
        pass
    
    def run(self) -> None:
        data_ = self.extract()
        df = self.transform(data_, self.transform_steps)
        return df

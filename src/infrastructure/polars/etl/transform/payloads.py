import polars as pl
from src.templates.etl.transform.base import TransformStep


class SelectPayloads(TransformStep):
    def __init__(self, columns: list[str]):
        self.columns = columns


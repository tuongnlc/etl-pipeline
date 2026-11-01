from dataclasses import dataclass


@dataclass
class ParquetExtractorWithSparkConfig:
    catalog: str
    schema: str
    volume: str
    path: str

    @property
    def parquet_uri(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.path}"

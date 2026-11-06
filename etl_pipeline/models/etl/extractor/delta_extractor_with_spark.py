from dataclasses import dataclass


@dataclass
class DeltaExtractorWithSparkConfig:
    catalog: str
    schema: str
    table: str

    @property
    def table_uri(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

from dataclasses import dataclass


@dataclass
class DeltaLoaderWithSparkConfig:
    catalog: str
    schema: str
    table: str

    @property
    def delta_table_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

from dataclasses import dataclass
from typing import Optional


@dataclass
class DeltaLoaderWithSparkConfig:
    catalog: str
    schema: str
    table: str

    @property
    def delta_table_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass
class DeltaMergeLoaderWithSparkConfig:
    catalog: str
    schema: str
    table: str
    merge_keys: list[str]
    partition_column: Optional[str] | None = None

    @property
    def delta_table_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

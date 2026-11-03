from dataclasses import dataclass


@dataclass
class SqlTransformWithSparkConfig:
    sql_query: str

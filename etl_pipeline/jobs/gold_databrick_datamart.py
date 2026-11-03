from pyspark.sql import DataFrame

from etl_pipeline.infrastructure.spark.etl.datamart_builder import DatamartBuilder


class GoldDatabrickDatamartJob:
    """
        Pipeline to build Data Mart on Databricks

        The pipeline including:
        - Build datamart by running an ETL SQL query
    """
    def __init__(
        self, 
        datamart_builder: DatamartBuilder,
    ):
        self.datamart_builder = datamart_builder

    def run_sql(self, etl_sql: str) -> DataFrame:
        """
            Build data mart
        """
        return self.datamart_builder.build(etl_sql)

    def run(self, etl_sql: str) -> None:
        self.run_sql(etl_sql)

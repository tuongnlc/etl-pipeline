from pyspark.sql import SparkSession, DataFrame

from etl_pipeline.templates.etl.load.delta_loader import DeltaLoader



class DeltaLoaderWithSpark(DeltaLoader):
    def __init__(self, delta_table_name: str, spark: SparkSession):
        self.delta_table_name = delta_table_name
        self.spark = spark
        self.write_mode: str = "overwrite"
        

    def load(self, df: DataFrame):
        """
            Load data to delta table
        """
        df.write.format("delta").mode(self.write_mode).saveAsTable(self.delta_table_name)


class DeltaMergeLoaderWithSpark(DeltaLoaderWithSpark):
    """
        Load DataFrame vào Delta table theo cơ chế merge (insert-only):
        - Nếu merge_keys đã tồn tại trong target -> skip.
        - Nếu chưa tồn tại -> insert.
    """
    def __init__(self, delta_table_name: str, spark: SparkSession, merge_keys: list[str]):
        super().__init__(delta_table_name, spark)
        self.merge_keys = merge_keys

    def load(self, df: DataFrame):
        """
            Merge DataFrame vào delta table (chỉ insert bản ghi mới).
        """
        # `delta.tables` chỉ có sẵn trên Databricks runtime -> import tại đây.
        from delta.tables import DeltaTable

        target = DeltaTable.forName(self.spark, self.delta_table_name)
        merge_condition = " AND ".join(
            f"target.`{key}` = source.`{key}`" for key in self.merge_keys
        )
        (
            target.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

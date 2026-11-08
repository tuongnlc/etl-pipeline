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
    def __init__(
        self,
        delta_table_name: str,
        spark: SparkSession,
        merge_keys: list[str],
        partition_column: str | None = None,
    ):
        super().__init__(delta_table_name, spark)
        self.merge_keys = merge_keys
        # Cột partition của bảng target (nếu có), dùng để prune khi merge.
        self.partition_column = partition_column

    def load(self, df: DataFrame):
        """
            Merge DataFrame vào delta table (chỉ insert bản ghi mới).
        """
        # `delta.tables` chỉ có sẵn trên Databricks runtime -> import tại đây.
        from delta.tables import DeltaTable

        target = DeltaTable.forName(self.spark, self.delta_table_name)
        # Thêm cột partition vào điều kiện merge để Delta prune theo partition,
        # tránh scan toàn bộ bảng.
        condition_keys = list(self.merge_keys)
        if self.partition_column:
            condition_keys.append(self.partition_column)
        merge_condition = " AND ".join(
            f"target.`{key}` = source.`{key}`" for key in condition_keys
        )
        
        (
            target.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

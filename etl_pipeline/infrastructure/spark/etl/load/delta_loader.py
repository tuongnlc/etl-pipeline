from pyspark.sql import SparkSession, DataFrame



class DeltaLoaderWithSpark:
    def __init__(self, delta_table_name: str, spark: SparkSession):
        self.delta_table_name = delta_table_name
        self.spark = spark
        self.write_mode: str = "overwrite"
        

    def load(self, df: DataFrame):
        """
            Load data to delta table
        """
        df.write.format("delta").mode(self.write_mode).saveAsTable(self.delta_table_name)

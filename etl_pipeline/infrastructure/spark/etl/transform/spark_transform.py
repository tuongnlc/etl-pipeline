from pyspark.sql import DataFrame

from etl_pipeline.templates.etl.transform.base import BaseTransform, TransformStep


class SparkTransform(BaseTransform):
    """
        Apply the transform steps on a Spark DataFrame one by one.
    """
    def transform(self, df: DataFrame, transform_steps: list[TransformStep]) -> DataFrame:
        for step in transform_steps or []:
            df = step.transform(df)
        return df

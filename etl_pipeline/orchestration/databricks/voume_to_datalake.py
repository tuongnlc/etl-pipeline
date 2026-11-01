from pyspark.sql import SparkSession

from etl_pipeline.infrastructure.spark.etl.extract.parquet_extractor import ParquetExtractorWithSpark
from etl_pipeline.infrastructure.spark.etl.load.delta_loader import DeltaLoaderWithSpark
from etl_pipeline.models.etl.extractor.parquet_extractor_with_spark import ParquetExtractorWithSparkConfig
from etl_pipeline.models.etl.jobs.volume_to_datalake import VolumeToDataLakeConfig
from etl_pipeline.utils.jobargs import etl_job_args_utils


def main():
    """
        Entry point cho Databricks python_wheel_task.

        Config được nạp từ đường dẫn truyền qua tham số `--job_config`
        (xem etl_job_args_utils để biết cách parse).
    """
    args = etl_job_args_utils()
    job_config: VolumeToDataLakeConfig = args.job_config

    if not isinstance(job_config, VolumeToDataLakeConfig):
        raise ValueError("job_config must be of type VolumeToDataLakeConfig")

    if not isinstance(job_config.extractor, ParquetExtractorWithSparkConfig):
        raise ValueError("extractor must be of type ParquetExtractorWithSparkConfig")

    spark = SparkSession.builder.getOrCreate()

    extractor = ParquetExtractorWithSpark(
        parquet_uri=job_config.extractor.parquet_uri,
        spark=spark,
    )
    df = extractor.extract()
    df.show()

    loader = DeltaLoaderWithSpark(
        delta_table_name=job_config.loader.delta_table_name,
        spark=spark,
    )
    loader.load(df)


if __name__ == "__main__":
    main()

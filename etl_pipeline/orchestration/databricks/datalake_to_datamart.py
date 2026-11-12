from pyspark.sql import SparkSession

from etl_pipeline.infrastructure.spark.etl.datamart_builder import DatamartBuilder
from etl_pipeline.jobs.gold_databrick_datamart import GoldDatabrickDatamartJob
from etl_pipeline.models.etl.jobs.datalake_to_datamart import DataLakeToDatamartConfig
from etl_pipeline.models.etl.transform.sql_transform import SqlTransformWithSparkConfig
from etl_pipeline.utils.jobargs import etl_job_args_utils


def main():
    """
        Entry point cho Databricks python_wheel_task.

        Config được nạp từ đường dẫn truyền qua tham số `--job_config`
        (xem etl_job_args_utils để biết cách parse).
    """
    args = etl_job_args_utils()
    job_config: DataLakeToDatamartConfig = args.job_config

    if not isinstance(job_config, DataLakeToDatamartConfig):
        raise ValueError("job_config must be of type DataLakeToDatamartConfig")

    if job_config.transform is None:
        raise ValueError("transform is required for DataLake to Datamart job")

    transform_config = job_config.transform
    if not isinstance(transform_config, SqlTransformWithSparkConfig):
        raise ValueError("transform must be of type SqlTransformWithSparkConfig")

    spark = SparkSession.builder.getOrCreate()

    datamart_builder = DatamartBuilder(spark=spark)

    gold_databrick_datamart_job = GoldDatabrickDatamartJob(
        datamart_builder=datamart_builder,
    )
    gold_databrick_datamart_job.run(etl_sql=transform_config.sql_query)


if __name__ == "__main__":
    main()

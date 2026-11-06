from pyspark.sql import SparkSession

from etl_pipeline.infrastructure.crawler.extract.crawl_newspaper_url import CrawlNewspaperUrl
from etl_pipeline.infrastructure.spark.etl.extract.parquet_extractor import ParquetExtractorWithSpark
from etl_pipeline.infrastructure.spark.etl.load.delta_loader import DeltaLoaderWithSpark, DeltaMergeLoaderWithSpark
from etl_pipeline.jobs.crawl_newspaper.bronze_crawl_newspaper_url import BronzeCrawlNewspaperUrl
from etl_pipeline.jobs.silver_databrick_lakehouse import SilverDatabrickLakehouseJob
from etl_pipeline.models.etl.extractor.parquet_extractor_with_spark import ParquetExtractorWithSparkConfig
from etl_pipeline.models.etl.jobs.volume_to_datalake import VolumeToDataLakeConfig
from etl_pipeline.utils.jobargs import etl_job_args_utils
from etl_pipeline.models.etl.jobs.crawler.crawl_newspaper_url import CrawlNewspaperUrlJobConfig



def main():
    """
        Entry point cho Databricks python_wheel_task.

        Config được nạp từ đường dẫn truyền qua tham số `--job_config`
        (xem etl_job_args_utils để biết cách parse).
    """
    args = etl_job_args_utils()
    job_config: CrawlNewspaperUrlJobConfig = args.job_config

    if not isinstance(job_config, CrawlNewspaperUrlJobConfig):
        raise ValueError("job_config must be of type CrawlNewspaperUrlJobConfig")

    spark = SparkSession.builder.getOrCreate()

    extractor = CrawlNewspaperUrl(
        link=job_config.extractor.link,
        spark=spark,
    )

    # Transform steps được khai báo trong config (spark_config/newspaper_url.yaml).
    transform_steps = (
        job_config.transform.transform_steps
        if job_config.transform is not None
        else []
    )

    loader = DeltaMergeLoaderWithSpark(
        delta_table_name=job_config.loader.delta_table_name,
        spark=spark,
        merge_keys=job_config.loader.merge_keys,
    )

    bronze_crawl_newspaper_url_job = BronzeCrawlNewspaperUrl(
        extractor=extractor,
        transform_steps=transform_steps,
        loader=loader,
    )
    bronze_crawl_newspaper_url_job.run()


if __name__ == "__main__":
    main()

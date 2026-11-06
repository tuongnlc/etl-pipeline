from pyspark.sql import SparkSession

from etl_pipeline.infrastructure.crawler.crawl_newspaper.crawl_newspaper import CrawlNewspaper
from etl_pipeline.infrastructure.spark.etl.extract.delta_extractor import DeltaExtractorWithSpark
from etl_pipeline.infrastructure.spark.etl.load.delta_loader import DeltaMergeLoaderWithSpark
from etl_pipeline.infrastructure.spark.etl.transform.spark_transform import SparkTransform
from etl_pipeline.jobs.crawl_newspaper.silver_crawl_newspaper import SilverCrawlNewspaper
from etl_pipeline.models.etl.jobs.crawler.silver_crawl_newspaper import SilverCrawlNewspaperJobConfig
from etl_pipeline.utils.jobargs import etl_job_args_utils


def main():
    """
        Entry point cho Databricks python_wheel_task.

        Config được nạp từ đường dẫn truyền qua tham số `--job_config`
        (xem etl_job_args_utils để biết cách parse).
    """
    args = etl_job_args_utils()
    job_config: SilverCrawlNewspaperJobConfig = args.job_config

    if not isinstance(job_config, SilverCrawlNewspaperJobConfig):
        raise ValueError("job_config must be of type SilverCrawlNewspaperJobConfig")

    spark = SparkSession.builder.getOrCreate()

    extractor = DeltaExtractorWithSpark(
        table_uri=job_config.extractor.table_uri,
        spark=spark,
    )

    # Transform steps được khai báo trong config (spark_config/newspaper.yaml).
    transform_steps = (
        job_config.transform.transform_steps
        if job_config.transform is not None
        else []
    )
    

    # Crawler được khai báo trong config (spark_config/newspaper.yaml).
    crawler = CrawlNewspaper(spark=spark)

    loader = DeltaMergeLoaderWithSpark(
        delta_table_name=job_config.loader.delta_table_name,
        spark=spark,
        merge_keys=job_config.loader.merge_keys,
    )

    silver_crawl_newspaper_job = SilverCrawlNewspaper(
        extractor=extractor,
        transformer=SparkTransform(),
        transform_steps=transform_steps,
        crawler=crawler,
        loader=loader,
    )
    silver_crawl_newspaper_job.run()


if __name__ == "__main__":
    main()

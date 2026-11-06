from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType
from etl_pipeline.templates.etl.crawler.crawler_extractor import Crawler
import newspaper
from typing import Any
from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession


class CrawlNewspaper(Crawler):
    def __init__(self, spark: SparkSession, headless: bool = True, **kwargs: Any) -> None:
        self.spark = spark

    def crawl(self, df: DataFrame, **kwargs: Any):
        data_ = []

        #convert df to list of str
        urls = df.select("url").toPandas()["url"].tolist()
        
        for url in urls:
            article = newspaper.article(url, timeout=30)

            newspaper_title = article.title
            publish_date = article.publish_date
            # Chuẩn hóa về datetime naive để tránh lỗi trộn timezone khi build DataFrame
            if isinstance(publish_date, datetime) and publish_date.tzinfo is not None:
                publish_date = publish_date.astimezone(timezone.utc).replace(tzinfo=None)
            newspaper_content = article.text
            data_.append({
                "newspaper_title": newspaper_title,
                "newspaper_url": url,
                "publish_date": publish_date,
                "newspaper_content": newspaper_content,
            })
        print(data_)
        
        # Build spark dataframe with these data
        # Schema của DataFrame links (gom 2 cột: text, url).
        SCHEMA = StructType([
            StructField("newspaper_title", StringType(), True),
            StructField("newspaper_url", StringType(), True),
            StructField("publish_date", DateType(), True),
            StructField("newspaper_content", StringType(), True),
        ])

        df_crawled = self.spark.createDataFrame(data_, schema=SCHEMA)

        return df_crawled

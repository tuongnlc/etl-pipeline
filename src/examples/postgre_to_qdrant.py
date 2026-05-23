import os
from src.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars
from argparse import Namespace
# from pyspark.sql import SparkSession
# from spark.configlib.parser.silver_job import SilverJobConfig
from src.models.etl.extractor.postgres_extractor_with_polars import PostgreDBExtractorWithPolarsConfig
import os
from src.utils.jobargs import etl_job_args_utils
from src.models.etl.jobs.postgre_to_qdrant_bronze import PostgreToQdrantBronzeConfig
from src.infrastructure.polars.etl.load.qdrant_loader import QdrantLoader
from src.models.etl.loader.qdrant_loader import QdrantLoaderConfig
from src.utils.qdrant_payload_config_loader import build_payload_model
import polars as pl


def main(
    args: Namespace
):
    if not isinstance(args.job_config, PostgreToQdrantBronzeConfig): # Check job_config is of type SilverJobConfig  
        raise ValueError("job_config must be of type PostgreToQdrantBronzeConfig")

    if not isinstance(args.job_config.extractor, PostgreDBExtractorWithPolarsConfig):
        raise ValueError("extractor must be of type PostgreDBExtractorWithPolarsConfig")

    if not isinstance(args.job_config.loader, QdrantLoaderConfig):
        raise ValueError("loader must be of type QdrantLoaderConfig")

    extractor = PostgreDBExtractorWithPolars(
        source_table_name=args.job_config.extractor.source_table_name,
        uri=args.job_config.extractor.uri,
        extractor_column_filter=args.job_config.extractor.extractor_column_filter,
        filter_value=args.job_config.extractor.filter_value,
    )
    
    df = extractor.extract()
    if not isinstance(df, pl.DataFrame):
        df = pl.from_arrow(df)

    print(df)

    # class NewspaperPayload(BaseModel):
    #     id: str
    #     newspaper_title: str
    #     newspaper_url: str
    #     publish_date: Optional[date]    
    #     newspaper_content: str
    #     newspaper_summary: str
    #     is_load_to_qdrant: int
    #     created_at: date

    payload_model = build_payload_model(
        model_name="NewspaperPayload",
        payload_config=args.job_config.loader.qrant_payload,
    )

    loader = QdrantLoader(
        qrant_url=args.job_config.loader.qrant_url,
        collection_name=args.job_config.loader.collection_name,
        qrant_payload=payload_model
    )

    loader.load(df)
    print("Load data to qdrant database successfully")

    
if __name__ == "__main__":
    args = etl_job_args_utils()

    if os.getenv("ENV", "") == "local":
        main(args=args)
    else:
        main(args=args)   

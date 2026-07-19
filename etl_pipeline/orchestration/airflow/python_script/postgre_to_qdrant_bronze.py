from etl_pipeline.models.etl.jobs.postgre_to_qdrant_bronze import PostgreToQdrantBronzeConfig
from etl_pipeline.utils.config_loader import parse_config
from argparse import Namespace
from etl_pipeline.models.etl.extractor.postgres_extractor_with_polars import PostgreDBExtractorWithPolarsConfig
from etl_pipeline.models.etl.loader.qdrant_loader import QdrantLoaderConfig
from etl_pipeline.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars
from etl_pipeline.infrastructure.polars.etl.load.qdrant_loader import QdrantLoader
from etl_pipeline.jobs.bronze_postgre_qdrant import BronzePostgreQdrant
import os
from airflow.sdk.bases.hook import BaseHook
from etl_pipeline.infrastructure.polars.etl.transform.qdrant_transform import QdrantTransform


def main(
    job_config: PostgreToQdrantBronzeConfig = None,
    execution_date: str = None, #get from airflow
):
    if job_config.extractor is not None:
        job_config.extractor = parse_config(job_config.extractor)
    if job_config.transform is not None:
        job_config.transform = parse_config(job_config.transform)
        if getattr(job_config.transform, "transform_steps", None) is not None:
            job_config.transform.transform_steps = [
                parse_config(step) for step in job_config.transform.transform_steps
            ]
    if job_config.loader is not None:
        job_config.loader = parse_config(job_config.loader)
    args = Namespace(job_config=job_config, execution_date=execution_date)

    if not isinstance(args.job_config, PostgreToQdrantBronzeConfig): # Check job_config is of type SilverJobConfig  
        raise ValueError("job_config must be of type SilverJobConfig")

    if not isinstance(args.job_config.extractor, PostgreDBExtractorWithPolarsConfig):
        raise ValueError("extractor must be of type PostgreDBExtractorWithPolarsConfig")

    if not isinstance(args.job_config.loader, QdrantLoaderConfig):
        raise ValueError("loader must be of type QdrantLoaderConfig")

    polars_connection = BaseHook.get_connection('postgres_market_data_polar_uri')
    uri = polars_connection.password

    # #Update here
    if job_config.extractor.filter_type == 'date':
        execution_date_filter = execution_date
    else:
        execution_date_filter = None

    extractor = PostgreDBExtractorWithPolars(
        source_table_name=args.job_config.extractor.source_table_name,
        uri=uri,
        filter_type=job_config.extractor.filter_type,
        extractor_column_filter=args.job_config.extractor.extractor_column_filter,
        filter_value=args.job_config.extractor.filter_value,
        execution_date_filter=execution_date_filter,
        filter_time_range=job_config.extractor.filter_time_range,
    )

    qdrant_transform = QdrantTransform()

    loader = QdrantLoader(
        qdrant_url=args.job_config.loader.qdrant_url,
        destination_collection_name=args.job_config.loader.destination_collection_name,
    )

    bronze_postgre_qdrant_job = BronzePostgreQdrant(
        extractor=extractor,
        transform=qdrant_transform,
        transform_steps=args.job_config.transform.transform_steps,
        loader=loader,

    )
    bronze_postgre_qdrant_job.run()


if __name__ == "__main__":
    if os.getenv("ENV", "") == "local":
        main()
    else:
        main()

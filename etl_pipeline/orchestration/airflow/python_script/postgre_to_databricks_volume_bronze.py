import os
from argparse import Namespace

from airflow.sdk.bases.hook import BaseHook

from etl_pipeline.models.etl.jobs.postgre_to_databricks_volume_bronze import PostgreToDatabricksVolumeBronzeConfig
from etl_pipeline.models.etl.extractor.postgres_extractor_with_polars import PostgreDBExtractorWithPolarsConfig
from etl_pipeline.models.etl.loader.databricks_volume_loader import DatabricksVolumeLoaderConfig
from etl_pipeline.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars
from etl_pipeline.infrastructure.polars.etl.load.databricks_volume_loader import DatabricksVolumeLoader
from etl_pipeline.infrastructure.polars.etl.transform.qdrant_transform import QdrantTransform
from etl_pipeline.jobs.bronze_postgre_databricks import BronzePostgreDatabricks
from etl_pipeline.utils.config_loader import parse_config


def main(
    job_config: PostgreToDatabricksVolumeBronzeConfig = None,
    execution_date: str = None,  # get from airflow
):
    if job_config.extractor is not None:
        job_config.extractor = parse_config(job_config.extractor)
    if job_config.loader is not None:
        job_config.loader = parse_config(job_config.loader)

    args = Namespace(job_config=job_config, execution_date=execution_date)

    if not isinstance(args.job_config, PostgreToDatabricksVolumeBronzeConfig):
        raise ValueError("job_config must be of type PostgreToDatabricksVolumeBronzeConfig")

    if not isinstance(args.job_config.extractor, PostgreDBExtractorWithPolarsConfig):
        raise ValueError("extractor must be of type PostgreDBExtractorWithPolarsConfig")

    if not isinstance(args.job_config.loader, DatabricksVolumeLoaderConfig):
        raise ValueError("loader must be of type DatabricksVolumeLoaderConfig")

    polars_connection = BaseHook.get_connection("postgres_market_data_polar_uri")
    uri = polars_connection.password

    if job_config.extractor.filter_type == "date":
        execution_date_filter = execution_date
    else:
        execution_date_filter = None

    extractor = PostgreDBExtractorWithPolars(
        source_table_name=args.job_config.extractor.source_table_name,
        uri=uri,
        execution_date_filter=execution_date_filter,
    )

    loader = DatabricksVolumeLoader(
        volume_url=args.job_config.loader.volume_url,
        local_file_path=args.job_config.loader.local_file_path,
    )

    bronze_postgre_databricks_job = BronzePostgreDatabricks(
        extractor=extractor,
        transformer=QdrantTransform(),
        transform_steps=[],
        loader=loader,
    )
    bronze_postgre_databricks_job.run()


if __name__ == "__main__":
    if os.getenv("ENV", "") == "local":
        main()
    else:
        main()

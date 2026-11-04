import os
from argparse import Namespace


from etl_pipeline.models.etl.loader.databricks_volume_loader import DatabricksVolumeLoaderConfig
from etl_pipeline.infrastructure.polars.etl.load.databricks_volume_loader import DatabricksVolumeLoader
from etl_pipeline.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from etl_pipeline.infrastructure.polars.etl.transform.qdrant_transform import QdrantTransform
from etl_pipeline.jobs.bronze_postgre_databricks import BronzePostgreDatabricks
from etl_pipeline.utils.config_loader import parse_config
from etl_pipeline.models.etl.jobs.qdrant_to_databricks_volume_bronze import QdrantToDatabricksVolumeBronzeConfig
from etl_pipeline.models.etl.extractor.qdrant_extractor_with_payload import QdrantExtractorWithPayloadConfig


def main(
    job_config: QdrantToDatabricksVolumeBronzeConfig = None,
    execution_date: str = None,  # get from airflow
):
    if job_config.extractor is not None:
        job_config.extractor = parse_config(job_config.extractor)
    if job_config.loader is not None:
        job_config.loader = parse_config(job_config.loader)

    args = Namespace(job_config=job_config, execution_date=execution_date)

    if not isinstance(args.job_config, QdrantToDatabricksVolumeBronzeConfig):
        raise ValueError("job_config must be of type QdrantToDatabricksVolumeBronzeConfig")

    if not isinstance(args.job_config.extractor, QdrantExtractorWithPayloadConfig):
        raise ValueError("extractor must be of type QdrantExtractor")

    if not isinstance(args.job_config.loader, DatabricksVolumeLoaderConfig):
        raise ValueError("loader must be of type DatabricksVolumeLoaderConfig")


    extractor = QdrantExtractorWithPayloadFilter(
        qdrant_url=args.job_config.extractor.qdrant_url,
        collection_name=args.job_config.extractor.collection_name,
        payload_filter=args.job_config.extractor.payload_filter,
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

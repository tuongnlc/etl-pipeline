from models.etl.jobs.postgre_to_qdrant_bronze import PostgreToQdrantBronzeConfig
from src.utils.config_loader import parse_config
from argparse import Namespace
from src.models.etl.extractor.postgres_extractor_with_polars import PostgreDBExtractorWithPolarsConfig
from src.models.etl.loader.qdrant_loader import QdrantLoaderConfig
from src.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars
from src.utils.qdrant_payload_config_loader import build_payload_model
from src.infrastructure.polars.etl.load.qdrant_loader import QdrantLoader
from src.jobs.bronze_newspaper import BronzeNewspaper

def main(
    job_config: PostgreToQdrantBronzeConfig = None,
    # execution_date: str = None,
):
    if job_config.extractor is not None:
        job_config.extractor = parse_config(job_config.extractor)
    if job_config.loader is not None:
        job_config.loader = parse_config(job_config.loader)
    args = Namespace(job_config=job_config)

    if not isinstance(args.job_config, PostgreToQdrantBronzeConfig): # Check job_config is of type SilverJobConfig  
        raise ValueError("job_config must be of type SilverJobConfig")

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

    payload_model = build_payload_model(
        model_name="NewspaperPayload",
        payload_config=args.job_config.loader.qrant_payload,
    )

    loader = QdrantLoader(
        qrant_url=args.job_config.loader.qrant_url,
        collection_name=args.job_config.loader.collection_name,
        qrant_payload=payload_model
    )

    bronze_newspaper_jopb = BronzeNewspaper(
        extractor=extractor,
        loader=loader,
    )
    bronze_newspaper_jopb.run()
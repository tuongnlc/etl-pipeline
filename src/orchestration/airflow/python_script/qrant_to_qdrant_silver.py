from src.models.etl.jobs.qdrant_to_qdrant_silver import QdrantToQdrantSilverConfig
from src.utils.config_loader import parse_config
from argparse import Namespace
from src.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from src.infrastructure.polars.etl.transform.qdrant_transform import QdrantTransform
from src.jobs.silver_newspaper import SilverNewspaper
import os
from src.utils.qdrant_payload_config_loader import build_payload_model
from src.infrastructure.polars.etl.load.qdrant_loader import QdrantLoader


def main(
    job_config: QdrantToQdrantSilverConfig = None,
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
    
    args = Namespace(job_config=job_config)

    extract = QdrantExtractorWithPayloadFilter(
        qdrant_url = args.job_config.extractor.qrant_url,
        collection_name=args.job_config.extractor.collection_name,
        payload_filter=args.job_config.extractor.payload_filter,
    )

    qdrant_transform = QdrantTransform()

    payload_model = build_payload_model(
        model_name="NewspaperPayload",
        payload_config=args.job_config.loader.qrant_payload,
    )

    payload_for_source_table = args.job_config.loader.qrant_payload_for_source_table
    payload_filter_for_source_table = args.job_config.loader.payload_filter_for_source_table

    loader = QdrantLoader(
        qrant_url=args.job_config.loader.qrant_url,
        destination_collection_name=args.job_config.loader.destination_collection_name,
        qrant_payload=payload_model,
        is_upsert_source_table=args.job_config.loader.is_upsert_source_table,
        source_name=args.job_config.loader.source_name,
        qrant_payload_for_source_table=payload_for_source_table,
        payload_filter_for_source_table=payload_filter_for_source_table,
    )

    silver_newspaper_job = SilverNewspaper(
        extractor=extract,
        transform=qdrant_transform,
        transform_steps=args.job_config.transform.transform_steps,
        loader=loader,
    )

    silver_newspaper_job.run()

if __name__ == "__main__":
    if os.getenv("ENV", "") == "local":
        main()
    else:
        main()

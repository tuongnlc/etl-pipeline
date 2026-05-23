from src.models.etl.jobs.qdrant_to_qdrant_silver import QdrantToQdrantSilverConfig
from src.utils.config_loader import parse_config
from argparse import Namespace
from src.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from src.infrastructure.polars.etl.transform.qdrant_transform import QdrantTransform
from src.jobs.silver_newspaper import SilverNewspaper
import os


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
        qdrant_url = args.job_config.loader.qrant_url,
        collection_name=args.job_config.loader.collection_name,
        payload_filter=args.job_config.loader.payload_filter,
    )

    qdrant_transform = QdrantTransform()

    silver_newspaper_job = SilverNewspaper(
        extractor=extract,
        transform=qdrant_transform,
        transform_steps=args.job_config.transform.transform_steps,
    )

    silver_newspaper_job.run()

if __name__ == "__main__":
    if os.getenv("ENV", "") == "local":
        main()
    else:
        main()

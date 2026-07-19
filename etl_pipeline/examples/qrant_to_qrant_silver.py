import os
from argparse import Namespace
from etl_pipeline.jobs.silver_qdrant_embedding import SilverQdrantEmbedding
from etl_pipeline.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from etl_pipeline.infrastructure.polars.etl.transform.qdrant_transform import QdrantTransform
from etl_pipeline.utils.jobargs import etl_job_args_utils


def main(
    args: Namespace
):
    extract = QdrantExtractorWithPayloadFilter(
        qdrant_url = args.job_config.loader.qdrant_url,
        collection_name=args.job_config.loader.collection_name,
        payload_filter=args.job_config.loader.payload_filter,
    )

    qdrant_transform = QdrantTransform()

    silver_qdrant_embedding_job = SilverQdrantEmbedding(
        extractor=extract,
        transform=qdrant_transform,
        transform_steps=args.job_config.transform.transform_steps,
    )

    df = silver_qdrant_embedding_job.run()

    
if __name__ == "__main__":
    args = etl_job_args_utils()

    if os.getenv("ENV", "") == "local":
        main(args=args)
    else:
        main(args=args)   

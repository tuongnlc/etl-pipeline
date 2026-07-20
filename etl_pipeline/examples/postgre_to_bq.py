import argparse
import json
import os
from pathlib import Path

import yaml
from google.oauth2 import service_account

from etl_pipeline.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars
from etl_pipeline.infrastructure.polars.etl.load.bq_loader import BigQueryLoaderPolars

from argparse import Namespace
from typing import Optional
# from pyspark.sql import SparkSession
# from spark.configlib.parser.silver_job import SilverJobConfig
from etl_pipeline.models.etl.jobs.postgre_to_bq_silver import PostgreToBqSilverConfig
from etl_pipeline.models.etl.extractor.postgres_extractor_with_polars import PostgreDBExtractorWithPolarsConfig
from etl_pipeline.models.etl.loader.bq_loader_with_polars import BigQueryLoaderPolarsConfig
import os
from etl_pipeline.utils.jobargs import etl_job_args_utils

# d
def main(
    args: Namespace
):
    if not isinstance(args.job_config, PostgreToBqSilverConfig): # Check job_config is of type SilverJobConfig  
        raise ValueError("job_config must be of type SilverJobConfig")

    if not isinstance(args.job_config.extractor, PostgreDBExtractorWithPolarsConfig):
        raise ValueError("extractor must be of type PostgreDBExtractorWithPolarsConfig")
    if not isinstance(args.job_config.loader, BigQueryLoaderPolarsConfig):
        raise ValueError("loader must be of type BigQueryLoaderPolarsConfig")

    extractor = PostgreDBExtractorWithPolars(
        query=args.job_config.extractor.query,
        uri=args.job_config.extractor.uri,
    )
    
    df = extractor.extract()

    loader = BigQueryLoaderPolars(
        gcp_credential_key=args.job_config.loader.gcp_credential_key,
        project=args.job_config.loader.project,
        dataset=args.job_config.loader.dataset,
        table=args.job_config.loader.table,
    )

    loader.load(df)

    
if __name__ == "__main__":
    args = etl_job_args_utils()

    if os.getenv("ENV", "") == "local":
        main(args=args)
    else:
        main(args=args)   
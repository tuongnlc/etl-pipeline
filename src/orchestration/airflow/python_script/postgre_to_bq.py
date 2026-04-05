import json
import os
from pathlib import Path

from google.oauth2 import service_account

from src.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars
from src.infrastructure.polars.etl.load.bq_loader import BigQueryLoaderPolars
from src.jobs.silver_market_data import SilverMarketData


from argparse import Namespace

from src.models.etl.jobs.postgre_to_bq_silver import PostgreToBqSilverConfig
from src.models.etl.extractor.postgres_extractor_with_polars import PostgreDBExtractorWithPolarsConfig
from src.models.etl.loader.bq_loader_with_polars import BigQueryLoaderPolarsConfig
import os
from src.utils.config_loader import load_and_parse_config, parse_config
from airflow.sdk.bases.hook import BaseHook

def main(
    job_config_path: str = None,
):
    runtime_args = Namespace(job_config=job_config_path)
    job_config = load_and_parse_config(job_config_path, runtime_args)
    if job_config.extractor is not None:
        job_config.extractor = parse_config(job_config.extractor)
    if job_config.loader is not None:
        job_config.loader = parse_config(job_config.loader)
    args = Namespace(job_config=job_config)

    if not isinstance(args.job_config, PostgreToBqSilverConfig): # Check job_config is of type SilverJobConfig  
        raise ValueError("job_config must be of type SilverJobConfig")

    if not isinstance(args.job_config.extractor, PostgreDBExtractorWithPolarsConfig):
        raise ValueError("extractor must be of type PostgreDBExtractorWithPolarsConfig")
    if not isinstance(args.job_config.loader, BigQueryLoaderPolarsConfig):
        raise ValueError("loader must be of type BigQueryLoaderPolarsConfig")

    # Get connections inside the function (not at import time)
    polars_connection = BaseHook.get_connection('postgres_market_data_polar_uri')
    uri = polars_connection.password
    
    bq_connectyion = BaseHook.get_connection('gcp_sa_for_bq_data_append')
    json_credentials = json.loads(bq_connectyion.password)
    credentials = service_account.Credentials.from_service_account_info(json_credentials)

    extractor = PostgreDBExtractorWithPolars(
        query=args.job_config.extractor.query,
        uri=uri,
    )

    loader = BigQueryLoaderPolars(
        gcp_credential=credentials,
        project=args.job_config.loader.project,
        dataset=args.job_config.loader.dataset,
        table=args.job_config.loader.table,
    )

    silver_market_data_jobs = SilverMarketData(
        extractor=extractor,
        loader=loader,
    )
    silver_market_data_jobs.run()

    
if __name__ == "__main__":
    if os.getenv("ENV", "") == "local":
        main()
    else:
        main()   

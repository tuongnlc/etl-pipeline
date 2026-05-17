import os


from src.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars


from argparse import Namespace
# from pyspark.sql import SparkSession
# from spark.configlib.parser.silver_job import SilverJobConfig
from src.models.etl.jobs.postgre_to_bq_silver import PostgreToBqSilverConfig
from src.models.etl.extractor.postgres_extractor_with_polars import PostgreDBExtractorWithPolarsConfig
import os
from src.utils.jobargs import etl_job_args_utils
from src.models.etl.jobs.postgre_to_qdrant_bronze import PostgreToQdrantBronzeConfig
# d
def main(
    args: Namespace
):
    if not isinstance(args.job_config, PostgreToQdrantBronzeConfig): # Check job_config is of type SilverJobConfig  
        raise ValueError("job_config must be of type PostgreToQdrantBronzeConfig")

    if not isinstance(args.job_config.extractor, PostgreDBExtractorWithPolarsConfig):
        raise ValueError("extractor must be of type PostgreDBExtractorWithPolarsConfig")

    extractor = PostgreDBExtractorWithPolars(
        source_table_name=args.job_config.extractor.source_table_name,
        uri=args.job_config.extractor.uri,
        extractor_column_filter=args.job_config.extractor.extractor_column_filter,
        filter_value=args.job_config.extractor.filter_value,
    )
    
    df = extractor.extract()

    print(df)

    
if __name__ == "__main__":
    args = etl_job_args_utils()

    if os.getenv("ENV", "") == "local":
        main(args=args)
    else:
        main(args=args)   
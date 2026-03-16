from src.jobs.silver_market_data import SilverMarketData
# from templates.etl.extract.postgre_db import PostgreDBExtractorWithPolars
from src.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars
from src.infrastructure.polars.etl.load.bq_loader import BigQueryLoaderPolars
import os

uri = "postgresql://postgres:postgres@localhost:5432/market_data"
query = """
    SELECT id::text AS id,
        stock_id,
        company_name,
        capitalization,
        business_sector
        FROM public.company_name
 """

extractor = PostgreDBExtractorWithPolars(
    uri=uri,
    query=query,
)



from google.oauth2 import service_account

# Construct absolute path to the credentials file (assumed to be in the same directory as this script)
credentials_path = os.path.join(os.path.dirname(__file__), "rich-finance-2026-699b4f81618a.json")

credentials = service_account.Credentials.from_service_account_file(
    credentials_path
)

loader = BigQueryLoaderPolars(
    gcp_credential=credentials,
    project="rich-finance-2026",
    dataset="market_data",
    table="company_name",
)

silver_market_data_jobs = SilverMarketData(
    extractor=extractor,
    loader=loader,
)

silver_market_data_jobs.run()
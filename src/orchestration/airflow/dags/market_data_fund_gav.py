#query connection from ariflow connection

from airflow.hooks.base import BaseHook
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from src.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars
from google.oauth2 import service_account
import json

from src.infrastructure.polars.etl.load.bq_loader import BigQueryLoaderPolars
from src.jobs.silver_market_data import SilverMarketData

# Deffine extractor
polars_connection = BaseHook.get_connection('postgres_market_data_polar_uri')
uri = polars_connection.password
query = """
    SELECT id::text AS id,
        fund_id,
        stock_id,
        business_sector,
        gav,
        month
    FROM public.fund_gav
 """

extractor = PostgreDBExtractorWithPolars(
    uri=uri,
    query=query,
)

# Define loader
bq_connectyion = BaseHook.get_connection('gcp_sa_for_bq_data_append')
json_credentials = json.loads(bq_connectyion.password)
credentials = service_account.Credentials.from_service_account_info(json_credentials)

loader = BigQueryLoaderPolars(
    gcp_credential=credentials,
    project="rich-finance-2026",
    dataset="market_data",
    table="fund_gav",
)

silver_market_data_jobs = SilverMarketData(
    extractor=extractor,
    loader=loader,
)

def run_silver_market_data_jobs():
    silver_market_data_jobs.run()

with DAG(
    dag_id='market_data_fund_gav_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['market_data', 'fund_gav'],
) as dag:
    task1 = PythonOperator(
        task_id='run_silver_market_data_fund_gav_jobs',
        python_callable=run_silver_market_data_jobs
    )
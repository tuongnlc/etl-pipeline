import polars as pl
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

# Using the engine='adbc' parameter
df = pl.read_database_uri(query=query, uri=uri, engine="adbc")

# print(df)
print(type(df))
arrow_table = df.to_arrow()
print(type(arrow_table))

#load arrow table to bigquery
from google.cloud import bigquery
from google.oauth2 import service_account
import io
import pyarrow.parquet as pq
from google.api_core import exceptions as gax_exceptions

destination_table = "rich-finance-2026.market_data.company_name"

project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
if not project:
    project = destination_table.split(".")[0]

# credentials = None
# cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("GCP_CREDENTIALS")
# if cred_path:
#     credentials = service_account.Credentials.from_service_account_file(cred_path)

credentials = service_account.Credentials.from_service_account_file(
    "rich-finance-2026-39c30a8f2339.json"
)

client = bigquery.Client(project=project, credentials=credentials)
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
)

table = client.get_table(destination_table)
table_location = table.location
print("client.project:", client.project)
print("service_account:", getattr(credentials, "service_account_email", None))
print("table.location:", table_location)
print("table.dataset_id:", table.dataset_id)

load_arrow = getattr(client, "load_table_from_arrow", None)
try:
    if callable(load_arrow):
        job = load_arrow(arrow_table, destination_table, job_config=job_config, location=table_location)
    else:
        job_config.source_format = bigquery.SourceFormat.PARQUET
        buf = io.BytesIO()
        pq.write_table(arrow_table, buf)
        buf.seek(0)
        job = client.load_table_from_file(buf, destination_table, job_config=job_config, location=table_location)
    print("job started:", job.job_id)
    job.result()
    print("job done:", job.job_id)
except gax_exceptions.Forbidden as e:
    print("forbidden:", e)
    raise
except gax_exceptions.GoogleAPICallError as e:
    print("api_error:", e)
    raise

#rich-finance-2026.market_data.company_name

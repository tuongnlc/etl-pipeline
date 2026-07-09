from google.cloud import bigquery
import io
import pyarrow.parquet as pq
from etl_pipeline.templates.etl.load.bq_loader import BigQueryLoader
import pyarrow as pa
import polars as pl


class BigQueryLoaderPolars(BigQueryLoader):
    """
        Load arrow table to bigquery
    """
    def __init__(self, gcp_credential, project: str, dataset: str, table: str, write_disposition: str = "WRITE_APPEND") -> None:
        self.gcp_credential = gcp_credential
        self.project = project
        self.dataset = dataset
        self.table = table
        self.write_disposition = write_disposition
        self.client = bigquery.Client(project=project, credentials=gcp_credential)

    def load(self, records: pa.Table | pl.DataFrame) -> None:
        """
            Load arrow table to bigquery
        """
        arrow_table = records.to_arrow() if isinstance(records, pl.DataFrame) else records
        job_config = bigquery.LoadJobConfig(
            write_disposition=self.write_disposition,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER, #never automate create table
            source_format=bigquery.SourceFormat.PARQUET,
        )

        buf = io.BytesIO()
        pq.write_table(arrow_table, buf)
        buf.seek(0)
        destination_table = f"{self.project}.{self.dataset}.{self.table}"
        table_location = 'asia-southeast1' #All tables using same region
        job = self.client.load_table_from_file(buf, destination_table, job_config=job_config, location=table_location)
        return job.result()

        
        

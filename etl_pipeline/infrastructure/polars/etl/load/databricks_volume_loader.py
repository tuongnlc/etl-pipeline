import os
import polars as pl
from databricks.sdk import WorkspaceClient
from etl_pipeline.templates.etl.load.databricks_volume_loader import DatabricksVolumeLoader

from dotenv import load_dotenv
load_dotenv()


class DatabricksVolumeLoader(DatabricksVolumeLoader):
    """
        Convert dataframe to parquet then upload file to databricks volume
    """
    def __init__(self, volume_url: str, local_file_path: str):
        self.volume_url = volume_url
        self.local_file_path = local_file_path
        databricks_host = os.getenv("DATABRICKS_HOST")
        databricks_token = os.getenv("DATABRICKS_TOKEN")
        self.w = WorkspaceClient(
            host=databricks_host,
            token=databricks_token,
        )

    def load(self, records: pl.DataFrame) -> None:
        output_dir = os.path.dirname(self.local_file_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        volume_file_path = f"/Volumes/{self.volume_url.replace('.', '/')}/{os.path.basename(self.local_file_path)}"

        try:
            records.write_parquet(
                self.local_file_path,
                compression="snappy",
                use_pyarrow=True,
                pyarrow_options={
                    "coerce_timestamps": "us",
                    "allow_truncated_timestamps": True,
                },
            )

            with open(self.local_file_path, "rb") as f:
                self.w.files.upload(volume_file_path, f, overwrite=True)
        finally:
            if os.path.exists(self.local_file_path):
                os.remove(self.local_file_path)

        print("Upload completed successfully!")

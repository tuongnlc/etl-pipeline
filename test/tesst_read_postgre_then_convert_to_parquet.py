import os

from etl_pipeline.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars

from dotenv import load_dotenv

load_dotenv()

extractor = PostgreDBExtractorWithPolars(
    source_table_name="public.fs_balance_sheet_type_four",
    uri="postgresql://postgres:postgres@localhost:5432/market_data",
)

df_ = extractor.extract()
print(type(df_))
# print(df_.schema)

# convert polars dataframe to parquet
output_dir = "/Users/tuongnguyen/Desktop/projects/etl-pipeline/test/test_parquet"
os.makedirs(output_dir, exist_ok=True)

output_path = os.path.join(output_dir, "fs_balance_sheet_type_four.parquet")

df_.write_parquet(
    output_path,
    compression="snappy",
    use_pyarrow=True,
    pyarrow_options={
        "coerce_timestamps": "us",
        "allow_truncated_timestamps": True,
    },
)

print(f"Wrote {df_.height} rows to {output_path}")

# volume_parth 
volume_url = 'test_volume.test_volume.test_volume'

import os
from databricks.sdk import WorkspaceClient

# 1. Initialize the Databricks client
# (It automatically picks up DATABRICKS_HOST and DATABRICKS_TOKEN from environment variables)
databricks_host = os.getenv("DATABRICKS_HOST")
databricks_token = os.getenv("DATABRICKS_TOKEN")
w = WorkspaceClient()

# 2. Define your paths
local_file_path = output_path
# Format for volume path: /Volumes/<catalog>/<schema>/<volume>/<filename>
volume_file_path = f"/Volumes/{volume_url.replace('.', '/')}/{os.path.basename(output_path)}"

try:
    print(f"Uploading {local_file_path} to {volume_file_path}...")

    # 3. Open the local file in binary read mode and upload
    with open(local_file_path, "rb") as f:
        w.files.upload(volume_file_path, f, overwrite=True)

    print("Upload completed successfully!")

except Exception as e:
    print(f"An error occurred during upload: {e}")
import os

import polars as pl
from etl_pipeline.templates.etl.transform.base import TransformStep


class ConvertPolarsToParquet(TransformStep):
    def transform(self, 
            data: pl.DataFrame, 
            output_path: str,
        ) -> pl.DataFrame:
        """
            Convert polars dataframe to parquet format
        """
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        return data.write_parquet(
            output_path,
            compression="snappy",
            use_pyarrow=True,
            pyarrow_options={
                "coerce_timestamps": "us",
                "allow_truncated_timestamps": True,
            },
        )

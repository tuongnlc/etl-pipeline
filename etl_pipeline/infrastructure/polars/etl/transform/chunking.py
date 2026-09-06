import polars as pl
from etl_pipeline.templates.etl.transform.base import TransformStep
from typing import Any
import uuid
from etl_pipeline.infrastructure.polars.etl.transform.text_processing.chunk_factory import text_splitter_factory


class ChunkPolars(TransformStep):
    def __init__(self, 
            document_col_name: str,
            chunk_col_name: str = "chunk_content",
            splitter_type: str = "recursive_character", #Default splitter type for simple text chunking
            chunk_size: int = None,
            chunk_overlap: int = None,
        ) -> None:
        if chunk_size and chunk_overlap:
            self.text_splitter = text_splitter_factory(splitter_type, chunk_size, chunk_overlap)
        else:
            self.text_splitter = text_splitter_factory(splitter_type) 
        self.document_col_name = document_col_name
        self.chunk_col_name = chunk_col_name

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        if "id" not in df.columns:
            raise ValueError("Missing required column: id")

        passthrough_columns = [
            c for c in df.columns if c not in ("id", self.document_col_name)
        ]

        chunked = (
            df.select(
                pl.col("id")
                .map_elements(lambda v: str(v), return_dtype=pl.Utf8)
                .alias("document_id"),
                *(pl.col(c) for c in passthrough_columns),
                pl.col(self.document_col_name)
                .fill_null("")
                .cast(pl.Utf8)
                .map_elements(
                    lambda s: self.text_splitter.split_text(s),
                    return_dtype=pl.List(pl.Utf8),
                )
                .alias(self.chunk_col_name),
            )
            .explode(self.chunk_col_name)
            .with_columns(
                chunk_index=pl.col("document_id").cum_count().over("document_id") - 1,
            )
            .with_columns(
                id=pl.struct("document_id", "chunk_index").map_elements(
                    lambda col: str(
                        uuid.uuid5(
                            uuid.NAMESPACE_DNS,
                            f"{col['document_id']}:{col['chunk_index']}",
                        )
                    ),
                    return_dtype=pl.Utf8,
                )
            )
            .select(["id", "document_id", *passthrough_columns, self.chunk_col_name, "chunk_index"])
        )
        return chunked

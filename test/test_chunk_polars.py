import polars as pl
from src.templates.etl.transform.base import TransformStep
from langchain_text_splitters import RecursiveCharacterTextSplitter
from typing import Any
import uuid


text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1200,       
    chunk_overlap=200,     
    length_function=len, 
)

class ChunkPolars(TransformStep):
    def __init__(self, 
            text_splitter: Any,
            document_col_name: str,
            chunk_col_name: str = "chunk_content"
        ) -> None:
        self.text_splitter = text_splitter
        self.document_col_name = document_col_name
        self.chunk_col_name = chunk_col_name

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        if "id" not in df.columns:
            raise ValueError("Missing required column: id")

        chunked = (
            df.select(
                pl.col("id").cast(pl.Utf8).alias("document_id"),
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
                id=pl.int_range(0, pl.len()).map_elements(
                    lambda _: str(uuid.uuid4()),
                    return_dtype=pl.Utf8,
                ),
            )
            .select(["id", "document_id", self.chunk_col_name, "chunk_index"])
        )

        return chunked

#import QdrantExtractorWithPayloadFilter
from src.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter

qdrant_url = 'http://localhost:6333'
collection_name = 'newspaper'
payload_filter = {
    "is_load_to_qdrant": 0
}

extractor = QdrantExtractorWithPayloadFilter(
    qdrant_url = qdrant_url,
    collection_name=collection_name,
    payload_filter=payload_filter,
)

df = extractor.extract()
# print(df)

# #select column
df = df.select(pl.col("id"), pl.col("newspaper_content"))

# # do chunk
chunk_polars = ChunkPolars(text_splitter, document_col_name="newspaper_content")
df = chunk_polars.transform(df)

df = df.filter(pl.col("document_id") == '1437310cad924556a96fd4882af0d473')
print(df.head())
# print(df)
# print(len(df))

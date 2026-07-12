from etl_pipeline.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from etl_pipeline.infrastructure.polars.etl.transform.chunking import ChunkPolars
from etl_pipeline.infrastructure.polars.etl.transform.columns import SelectColumns
from etl_pipeline.infrastructure.polars.etl.transform.clean_text import CleanTextPolars
from etl_pipeline.infrastructure.polars.etl.transform.text_processing.embedded_factory import chunk_embedding_factory
from etl_pipeline.infrastructure.polars.etl.transform.split_dataframe import SplitDataFrameTransformStep



qdrant_extractor = QdrantExtractorWithPayloadFilter(
    qdrant_url="http://localhost:6333",
    collection_name="newspaper",
    payload_filter={"is_embedded": 0},

)

raw_df = qdrant_extractor.extract()
print(len(raw_df))

qdrant_select_columns = SelectColumns(
    columns=["id", "publish_date", "newspaper_content"],
)
raw_df = qdrant_select_columns.transform(raw_df)
print(len(raw_df))

clean_text_polars = CleanTextPolars(
    col_name="newspaper_content",
)
raw_df = clean_text_polars.transform(raw_df)
print(len(raw_df))

chunk_polars = ChunkPolars(
    document_col_name="newspaper_content",
    chunk_col_name="chunk_content",
    splitter_type="recursive_character",
    chunk_size=1200,
    chunk_overlap=300,
)
raw_df = chunk_polars.transform(raw_df)
print(len(raw_df))

import polars as pl

def split_dataframe(df: pl.DataFrame, n: int) -> list[pl.DataFrame]:
    """Chia DataFrame thành n phần tuần tự bằng nhau nhất có thể."""
    q, r = divmod(len(df), n)
    return [df.slice(i * q + min(i, r), q + (1 if i < r else 0)) for i in range(n)]

# -------------------------------------------------------------------------
# Sử dụng thực tế:
# df = pl.DataFrame({"id": list(range(1, 11)), "val": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]})

split_dataframe_transform_step = SplitDataFrameTransformStep()
dataframes = split_dataframe_transform_step.transform(raw_df, n=3)

for df in dataframes:
    print(len(df))

EMBEDDING_CONFIG = {
    "embedding_type": "google_embedding",
    "model": "gemini-embedding-2",
    "task_type_documents": "RETRIEVAL_DOCUMENT",
    "task_type_query": "RETRIEVAL_QUERY",
    "output_dimensionality": 768,
    "batch_size": 100,
}


def embed_partition(df: pl.DataFrame, api_key: str) -> pl.DataFrame:
    embedder = chunk_embedding_factory(api_key=api_key, **EMBEDDING_CONFIG)
    return embedder.embed_documents(
        df=df,
        text_column="chunk_content",
        output_column="chunk_embedded",
    )


# dataframes = split_dataframe(raw_df, 3)
embedded_partitions = [
    embed_partition(df, api_key) for df, api_key in zip(dataframes, api_keys)
]

embedded_df = pl.concat(embedded_partitions, how="vertical")

print(len(embedded_df))
print(embedded_df.columns)

from etl_pipeline.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from etl_pipeline.infrastructure.polars.etl.transform.text_processing.embedded_factory import chunk_embedding_factory
from etl_pipeline.infrastructure.polars.etl.transform.text_processing.tokenize_vi import TokenizeVi
from etl_pipeline.infrastructure.polars.etl.load.qdrant_loader import QdrantLoader

sparse_embedder = chunk_embedding_factory(
    embedding_type="sparse_embedding",
    model="Qdrant/bm25",
)

extract = QdrantExtractorWithPayloadFilter(
    qdrant_url="http://localhost:6333",
    collection_name="test_collection",
    payload_filter={},
    # with_vectors=["dense"],
    with_vectors=False,
    ưith_payload=True
)
df_ = extract.extract()
df_ = df_.rename({"vector": "dense_vector_embedded"})

print(df_.head())
tokenize_vi = TokenizeVi(
    col_name="chunk_content",
    tokenize_col_name="chunk_content_vi_tokenized",
)
df_ = tokenize_vi.transform(df_)
df_ = sparse_embedder.embed_documents(
    df_,
    text_column="chunk_content_vi_tokenized",
)
df_ = df_.drop("chunk_content_vi_tokenized")

qdrant_loader = QdrantLoader(
    qdrant_url="http://localhost:6333",
    destination_collection_name="newspaper_embedded",
)

qdrant_loader.load(
    df_,
    dense_vector_column="dense_vector_embedded",
    sparse_vector_indices_column="sparse_vector_indices",
    sparse_vector_values_column="sparse_vector_value",
)

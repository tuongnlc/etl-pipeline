from etl_pipeline.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from etl_pipeline.infrastructure.polars.etl.transform.text_processing.embedded_factory import chunk_embedding_factory
from etl_pipeline.infrastructure.polars.etl.transform.text_processing.tokenize_vi import TokenizeVi
# from etl_pipeline.templates.etl.load.qdrant_loader import QdrantLoader
from etl_pipeline.infrastructure.polars.etl.load.qdrant_loader import QdrantLoader


sparse_embedder =chunk_embedding_factory(
    embedding_type="sparse_embedding",
    model="Qdrant/bm25",
)

# print(embedding_type)

extract = QdrantExtractorWithPayloadFilter(
    qdrant_url="http://localhost:6333",
    collection_name="newspaper_embedded",
    payload_filter={"publish_date":"2026-06-26"},
)
df_ = extract.extract()
print(len(df_)) 

tokenize_vi = TokenizeVi(col_name="chunk_content", tokenize_col_name="chunk_content_vi_tokenized")
df_ = tokenize_vi.transform(df_)


sparse_embedded_df = sparse_embedder.embed_documents(df_, text_column="chunk_content_vi_tokenized")
print(len(sparse_embedded_df)) 
print(sparse_embedded_df.limit(5))

dense_embedder = chunk_embedding_factory(
    embedding_type="google_embedding",
    model="gemini-embedding-2",
    api_key=""
    task_type_documents="RETRIEVAL_DOCUMENT",
    task_type_query="RETRIEVAL_QUERY",
    output_dimensionality=768,
    batch_size=100,
)
sparse_embedded_df = sparse_embedded_df.limit(5)
dense_embedded_df = dense_embedder.embed_documents(sparse_embedded_df, text_column="chunk_content", output_column="dense_vector_embedded")
print(dense_embedded_df)
# print(len(dense_embedded_df))     
# print(dense_embedded_df.limit(5))

#load to qdrant
qdrant_loader = QdrantLoader(
    qdrant_url="http://localhost:6333",
    destination_collection_name="test_collection",
)

qdrant_loader.load(
    dense_embedded_df,
    dense_vector_column="dense_vector_embedded",
    sparse_vector_indices_column="sparse_vector_indices",
    sparse_vector_values_column="sparse_vector_value",
)
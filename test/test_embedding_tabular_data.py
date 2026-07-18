from fastembed import TextEmbedding
from etl_pipeline.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from etl_pipeline.infrastructure.polars.etl.transform.text_processing.embedded_factory import chunk_embedding_factory
from etl_pipeline.infrastructure.polars.etl.transform.text_processing.tokenize_vi import TokenizeVi
from etl_pipeline.infrastructure.polars.etl.load.qdrant_loader import QdrantLoader
# TokenizeVi
import polars as pl


extract = QdrantExtractorWithPayloadFilter(
    qdrant_url="http://localhost:6333",
    collection_name="stock_price_embedded",
    payload_filter={
        "is_embedded": 0,
        "document_type": "income_statement",
    },
    # with_vectors=["dense"],
    # with_payload=True,
    with_vectors=False
)

df_ = extract.extract()
# df_ = df_.limit(5)
print(len(df_))

# #sparse embedding
# sparse_embedder = chunk_embedding_factory(
#     embedding_type="sparse_embedding",
#     model="Qdrant/bm25",
# )

# tokenize_vi = TokenizeVi(
#     col_name="chunk_content",
#     tokenize_col_name="chunk_content_vi_tokenized",
# )
# df_ = tokenize_vi.transform(df_)
# print(df_)
# print(df_.select('chunk_content_vi_tokenized'))

# df_ = sparse_embedder.embed_documents(
#     df_,
#     text_column="chunk_content_vi_tokenized",
# )
# print(df_.to_dict(as_series=False))
# for i in df_.select('chunk_content_vi_tokenized').to_dict():
#     print(i)
    # break
# df_ = df_.drop("chunk_content_vi_tokenized")

# print(df_)



# # #dense embedding
# dense_embedder = chunk_embedding_factory(
#     embedding_type="google_embedding",
#     model="gemini-embedding-2",
#     api_key='AQ.Ab8RN6IM8sA6Tq8YgkTPxoGYGBJvoDLv7Fk9DaVeV9QAmxQUMg',
#     task_type_documents="RETRIEVAL_DOCUMENT",
#     task_type_query="RETRIEVAL_QUERY",
#     output_dimensionality=768,
#     batch_size=100,
# )


# df_ = dense_embedder.embed_documents(
#     df_,
#     text_column="chunk_content",
#     output_column="dense_vector_embedded",
# )
# df_ = df_.with_columns(pl.lit(1, dtype=pl.Int8).alias("is_embedded"))
# print(df_)

# # load to qdrant 
# qdrant_loader = QdrantLoader(
#     qdrant_url="http://localhost:6333",
#     destination_collection_name="stock_price_embedded",
# )
# qdrant_loader.load(
#     df_,
#     dense_vector_column="dense_vector_embedded",
#     sparse_vector_indices_column="sparse_vector_indices",
#     sparse_vector_values_column="sparse_vector_value",
# )

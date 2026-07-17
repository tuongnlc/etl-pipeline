from etl_pipeline.templates.etl.transform.base import TransformStep
import os
import polars as pl
from etl_pipeline.infrastructure.polars.etl.transform.text_processing.embedded_factory import chunk_embedding_factory
from airflow.hooks.base import BaseHook
from etl_pipeline.utils.split_polar_df import split_dataframe


class TextEmbedding(TransformStep):
    def __init__(self,
        embedding_type: str = "google_embedding",
        model: str = "gemini-embedding-2",
        task_type_documents: str = "RETRIEVAL_DOCUMENT",
        task_type_query: str = "RETRIEVAL_QUERY",
        output_dimensionality: int = 768,
        batch_size: int = 100,
        text_column: str = "chunk_content",
        output_column: str = "dense_vector_embedded",  
        number_of_small_dataframes: int = 8, #Split df into 3 to avoid quota limit
    ):
        self.embedding_type = embedding_type
        self.model = model
        self.task_type_documents = task_type_documents
        self.task_type_query = task_type_query
        self.output_dimensionality = output_dimensionality
        self.batch_size = batch_size
        self.text_column = text_column
        self.output_column = output_column
        self.number_of_small_dataframes = number_of_small_dataframes

    def _extract_api_key(self, connection_name: str) -> str:
        """
            Get API from client secret in airflow connection
        """
        connection = BaseHook.get_connection(connection_name)
        extra_data = connection.get_extra_dejson()
        api_key = extra_data.get("client_secret")
        return api_key

    def _resolve_api_key(self) -> str:
        """
            Get API from client secret in airflow connection
        """
        api_keys = []

        if self.embedding_type == "google_embedding":
            api_key_1 = self._extract_api_key("google_gemini_embedding_key_1")
            api_key_2 = self._extract_api_key("google_gemini_embedding_key_2")
            api_key_3 = self._extract_api_key("google_gemini_embedding_key_3")
            api_key_4 = self._extract_api_key("google_gemini_embedding_key_4")
            api_key_5 = self._extract_api_key("google_gemini_embedding_key_5")
            api_key_6 = self._extract_api_key("google_gemini_embedding_key_6")
            api_key_7 = self._extract_api_key("google_gemini_embedding_key_7")
            # api_key_8 = self._extract_api_key("google_gemini_embedding_key_8")

            api_keys.append(api_key_1)
            api_keys.append(api_key_2)
            api_keys.append(api_key_3)
            api_keys.append(api_key_4)
            api_keys.append(api_key_5)
            api_keys.append(api_key_6)
            api_keys.append(api_key_7)
            # api_keys.append(api_key_8)
        return api_keys

    def transform(self, df: pl.DataFrame,  **kwargs) -> pl.DataFrame:
        dataframes = split_dataframe(df, n=self.number_of_small_dataframes)

        api_keys = self._resolve_api_key() 

        #Cretae embedding configs
        EMBEDDING_CONFIG = {
            "embedding_type": self.embedding_type,
            "model": self.model,
            "task_type_documents": self.task_type_documents,
            "task_type_query": self.task_type_query,
            "output_dimensionality": self.output_dimensionality,
            "batch_size": self.batch_size,
        }

        def embed_partition(df: pl.DataFrame, api_key: str) -> pl.DataFrame:
            embedder = chunk_embedding_factory(api_key=api_key, **EMBEDDING_CONFIG)
            return embedder.embed_documents(
                df=df,
                text_column="chunk_content",
                output_column="dense_vector_embedded",
            )

        embedded_partitions = [
            embed_partition(df, api_key) for df, api_key in zip(dataframes, api_keys)
        ]

        output_df = pl.concat(embedded_partitions, how="vertical")

        return output_df


class SparseTextEmbedding(TextEmbedding):
    def __init__(self, 
            embedding_type="sparse_embedding",
            model_name="Qdrant/bm25",
            **kwargs):
        super().__init__(**kwargs)
        self.embedding_type = embedding_type
        self.model_name = model_name

    def transform(self, df: pl.DataFrame,  **kwargs) -> pl.DataFrame:
        sparse_embedder = chunk_embedding_factory(
            embedding_type=self.embedding_type,
            model=self.model_name
        )

        sparse_embedded_df = sparse_embedder.embed_documents(df=df, text_column="chunk_content_vi_tokenized")

        sparse_embedded_df = sparse_embedded_df.drop("chunk_content_vi_tokenized") #Drop tokenized column 
        return sparse_embedded_df
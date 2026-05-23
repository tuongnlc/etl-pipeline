from src.templates.etl.transform.base import TransformStep
import os
import polars as pl
from src.infrastructure.polars.etl.transform.text_processing.embedded_factory import chunk_embedding_factory
from airflow.hooks.base import BaseHook


class TextEmbedding(TransformStep):
    def __init__(self,
        embedding_type: str = "google_embedding",
        model: str = "gemini-embedding-2",
        task_type_documents: str = "RETRIEVAL_DOCUMENT",
        task_type_query: str = "RETRIEVAL_QUERY",
        output_dimensionality: int = 768,
        batch_size: int = 100,
        text_column: str = "chunk_content",
        output_column: str = "chunk_embedded",        
    ):
        self.embedding_type = embedding_type
        self.model = model
        self.task_type_documents = task_type_documents
        self.task_type_query = task_type_query
        self.output_dimensionality = output_dimensionality
        self.batch_size = batch_size
        self.text_column = text_column
        self.output_column = output_column

    def _resolve_api_key(self) -> str:
        """
            Get API from client secret in airflow connection
        """
        if self.embedding_type == "google_embedding":
            connection = BaseHook.get_connection("google_gemini_api_key")
            extra_data = connection.get_extra_dejson()
            print(extra_data)
            # print(connection)
            # api_key = connection.get('password')
            # return api_key
            api_key = extra_data.get("client_secret")
            return api_key

    def transform(self, df: pl.DataFrame,  **kwargs) -> pl.DataFrame:
        api_key = self._resolve_api_key()
        embedding = chunk_embedding_factory(
            embedding_type=self.embedding_type, 
            api_key=api_key,
            model=self.model, 
            task_type_documents=self.task_type_documents,
            task_type_query=self.task_type_query,
            output_dimensionality=self.output_dimensionality,
            batch_size=self.batch_size,
        )

        return embedding.embed_documents(
            df=df,
            text_column=self.text_column, 
            output_column=self.output_column
        )

from langchain_core.embeddings import Embeddings
from typing import Optional, List, Dict, Type
from google.genai import Client, types
import polars as pl
from google.genai.errors import ClientError
import re
import time
from fastembed import SparseTextEmbedding as FastEmbedSparseTextEmbedding


class GoogleGeminiEmbedding(Embeddings):
    """
        Send a batch with 100 request for gemini
    """
    def __init__(
        self,
        api_key: str,
        model: str = "gemini-embedding-2",
        task_type_documents: str = "RETRIEVAL_DOCUMENT",
        task_type_query: str = "RETRIEVAL_QUERY",
        output_dimensionality: Optional[int] = None,
        batch_size: int = 100,
        max_retries: int = 5,
    ) -> None:
        self.client = Client(api_key=api_key)
        self.model = model
        self.task_type_documents = task_type_documents
        self.task_type_query = task_type_query
        self.output_dimensionality = output_dimensionality
        self.batch_size = min(batch_size, 100)
        self.max_retries = max_retries

    def _build_config(self, task_type: str) -> types.EmbedContentConfig:
        return types.EmbedContentConfig(
            task_type=task_type,
            output_dimensionality=self.output_dimensionality,
        )

    def _retry_delay_seconds(self, exc: Exception) -> Optional[float]:
        message = str(exc)
        match = re.search(r"retry in ([0-9]+(?:\.[0-9]+)?)s", message, flags=re.IGNORECASE)
        if match:
            return float(match.group(1))

        match = re.search(r"retryDelay': '(\d+)s'", message)
        if match:
            return float(match.group(1))

        return None

    def _embed_texts_one_request(
        self, texts: List[str], config: types.EmbedContentConfig
    ) -> List[List[float]]:
        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                result = self.client.models.embed_content(
                    model=self.model,
                    contents=[
                        types.UserContent(parts=[types.Part(text=text)])
                        for text in texts
                    ],
                    config=config,
                )
                return [list(item.values) for item in result.embeddings]
            except ClientError as exc:
                last_exc = exc
                message = str(exc)
                is_rate_limited = "RESOURCE_EXHAUSTED" in message or " 429 " in f" {message} "
                if not is_rate_limited or attempt >= self.max_retries:
                    raise

                delay = self._retry_delay_seconds(exc)
                time.sleep(delay if delay is not None else 60.0)

        raise last_exc if last_exc is not None else RuntimeError("Embedding failed without an exception.")

    def _batched(self, texts: List[str]) -> List[List[str]]:
        return [
            texts[i : i + self.batch_size]
            for i in range(0, len(texts), self.batch_size)
        ]

    def embed_documents(
        self,
        df: pl.DataFrame,
        *,
        text_column: str,
        output_column: str,
    ) -> pl.DataFrame:
        texts = ["" if v is None else str(v) for v in df[text_column].to_list()]
        if not texts:
            return df.with_columns(
                pl.Series(output_column, [], dtype=pl.List(pl.Float64))
            )

        config = self._build_config(self.task_type_documents)
        all_embeddings: List[List[float]] = []
        for batch in self._batched(texts):
            all_embeddings.extend(self._embed_texts_one_request(batch, config))

        df = df.with_columns(pl.Series(output_column, all_embeddings))
        print(df)
        return df

    def embed_query(
        self, text: str
    ) -> List[float]:
        config = self._build_config(self.task_type_query)

        result = self.client.models.embed_content(
            model=self.model,
            contents=[types.UserContent(parts=[types.Part(text=text)])],
            config=config,
        )

        return list(result.embeddings[0].values)


class SparseTextEmbedding(Embeddings):
    def __init__(
        self,
        model: str = "Qdrant/bm25",
        disable_stemmer: bool = True,
    ) -> None:
        self.model = model
        self.disable_stemmer = disable_stemmer
        self.embedding_model = FastEmbedSparseTextEmbedding(
            model_name=model,
            disable_stemmer=disable_stemmer,
        )

    def embed_query(self, text: str) -> List[float]:
        sparse_vector = next(self.embedding_model.query_embed(text))
        return sparse_vector.values.tolist()

    def embed_documents(
        self,
        df: pl.DataFrame,
        *,
        text_column: str,
        output_indices_column: str = "sparse_vector_indices",
        output_values_column: str = "sparse_vector_value",
    ) -> pl.DataFrame:
        if text_column not in df.columns:
            raise ValueError(f"Column '{text_column}' not found in dataframe")

        texts = ["" if value is None else str(value) for value in df[text_column].to_list()]

        if not texts:
            raise ValueError("No text to embed")

        sparse_vectors = list(self.embedding_model.embed(texts))
        df = df.with_columns(
            pl.Series(
                output_indices_column,
                [vec.indices.tolist() for vec in sparse_vectors],
                strict=False,
            ),
            pl.Series(
                output_values_column,
                [vec.values.tolist() for vec in sparse_vectors],
                strict=False,
            ),
        )
        return df


MAPPING_Embedding = {
    "google_embedding": GoogleGeminiEmbedding,
    "sparse_embedding": SparseTextEmbedding,
}

class EmbeddingFactory:
    _registry: Dict[str, Type[Embeddings]] = MAPPING_Embedding

    @classmethod
    def create(
        cls,
        embedding_type: str,
        *,
        api_key: Optional[str] = None, # For sparse embedding we don't need api_key
        model: str = "gemini-embedding-2",
        task_type_documents: str = "RETRIEVAL_DOCUMENT",
        task_type_query: str = "RETRIEVAL_QUERY",
        output_dimensionality: Optional[int] = None,
        batch_size: int = 100,
    ) -> Embeddings:
        key = embedding_type.strip().lower() #get embedding type to create embedding
        embedding_cls = cls._registry.get(key)
        if embedding_cls is None:
            raise ValueError(f"Unknown embedding_type: {embedding_type}")

        if embedding_type == "sparse_embedding":
            return embedding_cls(model=model)

        return embedding_cls(
            api_key=api_key,
            model=model,
            task_type_documents=task_type_documents,
            task_type_query=task_type_query,
            output_dimensionality=output_dimensionality,
            batch_size=batch_size,
        )

def chunk_embedding_factory(
    embedding_type: str,
    model: str,
    api_key: Optional[str] = None,
    task_type_documents: Optional[str] = None,
    task_type_query: Optional[str] = None,
    output_dimensionality: Optional[int] = None,
    batch_size: Optional[int] = None,
) -> Embeddings:
    return EmbeddingFactory.create(
        embedding_type,
        api_key=api_key,
        model=model,
        task_type_documents=task_type_documents,
        task_type_query=task_type_query,
        output_dimensionality=output_dimensionality,
        batch_size=batch_size,
    )

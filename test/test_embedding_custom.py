from __future__ import annotations

from typing import List, Optional

from google.genai import Client, types
from langchain_core.embeddings import Embeddings


class GeminiBatchEmbeddings(Embeddings):
    def __init__(
        self,
        api_key: str,
        model: str = "gemini-embedding-2-preview",
        task_type_documents: str = "RETRIEVAL_DOCUMENT",
        task_type_query: str = "RETRIEVAL_QUERY",
        output_dimensionality: Optional[int] = None,
        batch_size: int = 100,
    ) -> None:
        self.client = Client(api_key=api_key)
        self.model = model
        self.task_type_documents = task_type_documents
        self.task_type_query = task_type_query
        self.output_dimensionality = output_dimensionality
        self.batch_size = batch_size

    def _build_config(self, task_type: str) -> types.EmbedContentConfig:
        return types.EmbedContentConfig(
            task_type=task_type,
            output_dimensionality=self.output_dimensionality,
        )

    def _batched(self, texts: List[str]) -> List[List[str]]:
        return [
            texts[i : i + self.batch_size]
            for i in range(0, len(texts), self.batch_size)
        ]

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []

        all_embeddings: List[List[float]] = []
        config = self._build_config(self.task_type_documents)

        for batch in self._batched(texts):
            result = self.client.models.embed_content(
                model=self.model,
                contents=[
                    types.UserContent(parts=[types.Part(text=text)])
                    for text in batch
                ],
                config=config,
            )
            all_embeddings.extend([list(item.values) for item in result.embeddings])

        return all_embeddings

    def embed_query(self, text: str) -> List[float]:
        config = self._build_config(self.task_type_query)

        result = self.client.models.embed_content(
            model=self.model,
            contents=[types.UserContent(parts=[types.Part(text=text)])],
            config=config,
        )

        return list(result.embeddings[0].values)
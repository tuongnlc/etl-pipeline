from langchain_google_genai import GoogleGenerativeAIEmbeddings

import polars as pl
from src.templates.etl.transform.base import TransformStep
from langchain_text_splitters import RecursiveCharacterTextSplitter
from typing import Any
import uuid
import numpy as np
from langchain_core.embeddings import Embeddings
from typing import Optional, List, Dict, Type
from google.genai import Client, types
import polars as pl

#Define embedding
embeddings = GoogleGenerativeAIEmbeddings(
    model="gemini-embedding-2",
    api_key="AQ.Ab8RN6Jsbqvkq4R1Q7j9CK3JUyDR40dCj0mQajaJIzfOM5dMgA",
    dimensions=768,
    chunk_size=100
)

#Repair text to test
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
    "is_embedded": 1
}

extractor = QdrantExtractorWithPayloadFilter(
    qdrant_url = qdrant_url,
    collection_name=collection_name,
    payload_filter=payload_filter,
)

df = extractor.extract()

# # #select column
df = df.select(pl.col("id"), pl.col("newspaper_content"))

# # # # do chunk
chunk_polars = ChunkPolars(text_splitter, document_col_name="newspaper_content")
df = chunk_polars.transform(df)
# # print(df)

# ## convert tolist 
df_chunk_content = df.select(pl.col("chunk_content"))
# # print(df_chunk_content)

df_chunk_content_list = df_chunk_content['chunk_content'].to_list()
# # print(df_chunk_content_list)
take_1 = df_chunk_content_list[:1]
take_2 = df_chunk_content_list[:2] # take 2 request
take_10 = df_chunk_content_list[:10]
# print(len(take_2))
# print(len(str(take_2)))

# # vectors = embeddings.embed_documents(take_1)  #take 1 request to gemini , TPM = 258
# # print(vectors)

# vectors = embeddings.embed_documents(take_2)  #take 1 request to gemini , TPM = 258
# print("Đầu ra có bao nhiêu vector:", len(vectors))
# print("Mỗi vector có bao nhiêu chiều:", len(vectors[0]))
# # print(vectors)
# # print()


# arr = np.array(vectors)
# print(arr.shape)

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
    ) -> None:
        self.client = Client(api_key=api_key)
        self.model = model
        self.task_type_documents = task_type_documents
        self.task_type_query = task_type_query
        self.output_dimensionality = output_dimensionality
        self.batch_size = min(batch_size, 100)

    def _build_config(self, task_type: str) -> types.EmbedContentConfig:
        return types.EmbedContentConfig(
            task_type=task_type,
            output_dimensionality=self.output_dimensionality,
        )

    def _embed_texts_one_request(
        self, texts: List[str], config: types.EmbedContentConfig
    ) -> List[List[float]]:
        result = self.client.models.embed_content(
            model=self.model,
            contents=[
                types.UserContent(parts=[types.Part(text=text)])
                for text in texts
            ],
            config=config,
        )
        return [list(item.values) for item in result.embeddings]

    def _batched(self, texts: List[str]) -> List[List[str]]:
        batch_texts = [
            texts[i : i + self.batch_size]
            for i in range(0, len(texts), self.batch_size)
        ]
        print("do_dai_1_batch la", len(batch_texts))
        return batch_texts

    def embed_documents(
        self,
        df: pl.DataFrame,
        *,
        text_column: str = "chunk_content",
        output_column: str = "chunk_embedded",
    ) -> pl.DataFrame:
        texts = ["" if v is None else str(v) for v in df[text_column].to_list()]
        if not texts:
            return df.with_columns(
                pl.Series(output_column, [], dtype=pl.List(pl.Float64))
            )

        config = self._build_config(self.task_type_documents)
        all_embeddings: List[List[float]] = []
        for batch in self._batched(texts):
            one_request_call = self._embed_texts_one_request(batch, config)
            # print(f"Embedding batch: {len(batch)} -> {len(one_request_call)}")
            all_embeddings.extend(one_request_call)
        print(f"Total embeddings: {len(all_embeddings)}")

        df = df.with_columns(pl.Series(output_column, all_embeddings))
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

test_embeddings = GoogleGeminiEmbedding(api_key="AQ.Ab8RN6Jsbqvkq4R1Q7j9CK3JUyDR40dCj0mQajaJIzfOM5dMgA")
# test_1 = test_embeddings._batched(take_1)
# print(test_1)
# print(len(test_1))

test_2 = test_embeddings._batched(take_2)
print(test_2)
# print(len(test_2))
batches_10 = test_embeddings._batched(take_10)
print(batches_10)

# print(len(batches_10))
# print(len(str(batches_10))) #9162

config = test_embeddings._build_config(test_embeddings.task_type_documents)
test_embedded_10 = []

# # for i in range
# for batch in batches_10:
    # test_embedded_10.extend(test_embeddings._embed_texts_one_request(batch, config=config))
# print(batche)
# # print(test_embedded_10)
# # # print(len(test_embedded_2))
# # # convert to array
# arr = np.array(test_embedded_10)
# print(arr.shape)

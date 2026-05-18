from src.infrastructure.polars.etl.extract.postgre_db import PostgreDBExtractorWithPolars

from dotenv import load_dotenv
from typing import Optional

from pydantic import BaseModel
from datetime import date

from pydantic import BaseModel
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

load_dotenv()
#test query postgresql
loader = PostgreDBExtractorWithPolars(
    source_table_name="newspaper",
    uri="postgresql://postgres:postgres@localhost:5432/market_data",
    extractor_column_filter="is_load_to_qdrant",
    filter_value="0",
)

df = loader.extract()
# print(df)

# newspaper_payload = df.to_pandas()
class NewspaperPayload(BaseModel):
    id: str
    title: str
    url: str
    publish_date: Optional[date]    
    content: str
    summary: str
    is_embedded: int
    created_at: date

raw_data_list = df.to_pylist()
print(raw_data_list)
print(type(raw_data_list))

client = QdrantClient(url="http://localhost:6333")
collection_name = "collection_with_no_vector"

from pydantic import TypeAdapter
from typing import List

# Check payload validation
payload_list_adapter = TypeAdapter(List[NewspaperPayload])

# raw_payloads = [item["payload"] for item in raw_data_list]

try:
    validated_payloads = payload_list_adapter.validate_python(raw_data_list)
except Exception as e:
    raise ValueError(f"Payload validation failed: {e}") from e

points = [
    PointStruct(
        id=item["id"],
        vector={},
        payload=payload.model_dump(mode="json"),
    )
    for item, payload in zip(raw_data_list, validated_payloads)
]

client.upload_points(
    collection_name=collection_name,
    points=points,
    wait=False, # Set False để tăng tốc độ nếu không cần đọc ngay lập tức
    batch_size=2,
)

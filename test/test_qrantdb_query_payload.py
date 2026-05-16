from datetime import datetime
from pydantic import BaseModel
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from pydantic import ConfigDict

# 1. Định nghĩa Schema bằng Pydantic (Giữ nguyên của bạn)
class QdrantPayloadSchema(BaseModel):

    model_config = ConfigDict(extra="forbid")
    timestamp: datetime
    ticker: str
    is_active: bool
    volume: int

raw_data_list = [
    {"id": 1, "vector": [0.1] * 768, "payload": {"timestamp": "2026-05-16T12:00:00", "ticker": "AAPL", "is_active": True, "volume": 1500}},
    {"id": 2, "vector": [0.2] * 768, "payload": {"timestamp": "2026-05-16T13:00:00", "ticker": "MSFT", "is_active": True, "volume": 2500}},
    {"id": 3, "vector": [0.3] * 768, "payload": {"timestamp": "2026-05-16T14:00:00", "ticker": "GOOG", "is_active": False, "volume": 800}},
    # Giả sử có hàng ngàn bản ghi nữa ở đây...
]

batch_size = 2

client = QdrantClient(url="http://localhost:6333")
collection_name = "market_signals"

from pydantic import TypeAdapter
from typing import List

# Check payload validation
payload_list_adapter = TypeAdapter(List[QdrantPayloadSchema])

raw_payloads = [item["payload"] for item in raw_data_list]

try:
    validated_payloads = payload_list_adapter.validate_python(raw_payloads)
except Exception as e:
    raise ValueError(f"Payload validation failed: {e}") from e

points = [
    PointStruct(
        id=item["id"],
        vector=item["vector"],
        payload=payload.model_dump(mode="json"),
    )
    for item, payload in zip(raw_data_list, validated_payloads)
]

client.upload_points(
    collection_name=collection_name,
            points=points,
    wait=False, # Set False để tăng tốc độ nếu không cần đọc ngay lập tức
    batch_size=batch_size,
)

from qdrant_client import QdrantClient

client = QdrantClient(host="localhost", port=6333)
# or
client = QdrantClient(url="http://localhost:6333")

print(client.get_collections()) 

#create collection
from qdrant_client.models import Distance, VectorParams

client.create_collection(
    collection_name="test_collection_1",
    vectors_config=VectorParams(size=100, distance=Distance.COSINE),
)

#Test write data to qdrant collection
from pydantic import BaseModel
from typing import Optional

class StockPayload(BaseModel):
    ticker: str
    price: float
    volume: Optional[int] = None # Trường này có thể có hoặc không

def batch_upsert_with_validation(
    client: QdrantClient, 
    collection_name: str, 
    raw_data_list: List[dict], 
    vectors: List[list]
):
    # 1. Sử dụng List Comprehension hoặc loop để chuẩn bị danh sách points
    valid_points = []
    
    for i, (raw_item, vector) in enumerate(zip(raw_data_list, vectors)):
        try:
            # Validate từng item trước khi đưa vào hàng đợi batch
            validated_payload = StockPayload(**raw_item).model_dump()
            
            valid_points.append(
                PointStruct(
                    id=i, # Hoặc dùng logic ID của bạn
                    vector=vector,
                    payload=validated_payload
                )
            )
        except ValidationError as e:
            print(f"❌ Dòng {i} bị lỗi schema, bỏ qua: {e.json()}")

    # 2. Thực hiện BATCH UPSERT (Gửi toàn bộ danh sách trong 1 request)
    if valid_points:
        client.upsert(
            collection_name=collection_name,
            points=valid_points, # Truyền cả list vào đây
            wait=False # Set False để tăng tốc độ nếu không cần đọc ngay lập tức
        )
        print(f"🚀 Đã đẩy batch {len(valid_points)} points lên Qdrant thành công.")

# --- Cách sử dụng ---
# raw_data = [{"ticker": "KBC", "price": 32.1}, {"ticker": "VND", "price": 20.5}]
# vectors = [[0.1]*768, [0.5]*768]
# batch_upsert_with_validation(client, "stocks", raw_data, vectors)
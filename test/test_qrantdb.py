from qdrant_client import QdrantClient
from typing import List
from pydantic import ValidationError
from datetime import date

client = QdrantClient(host="localhost", port=6333)

#Test write data to qdrant collection
from pydantic import BaseModel
from typing import Optional
from qdrant_client.models import FieldCondition, Filter, MatchValue, PointStruct

class StockPayload(BaseModel):
    ticker: str
    price: float
    volume: Optional[int] = None # Trường này có thể có hoặc không
    trading_date: date

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
            point_id = raw_item["point_id"]
            payload_data = {key: value for key, value in raw_item.items() if key != "point_id"}
            validated_payload = StockPayload(**payload_data).model_dump(mode="json")
            
            valid_points.append(
                PointStruct(
                    id=point_id,
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

def fetch_points_in_qdrant_for_trading_day(
    client: QdrantClient,
    collection_name: str,
    target_date: date,
    page_size: int = 256,
) -> dict[int, tuple[dict, list]]:
    # Query phia Qdrant theo trading_date de lay snapshot cua ngay can dong bo.
    day_filter = Filter(
        must=[
            FieldCondition(
                key="trading_date",
                match=MatchValue(value=target_date.isoformat()),
            )
        ]
    )
    offset = None
    existing_points: dict[int, tuple[dict, list]] = {}

    while True:
        records, offset = client.scroll(
            collection_name=collection_name,
            scroll_filter=day_filter,
            limit=page_size,
            offset=offset,
            with_payload=True,
            with_vectors=True,
        )

        for record in records:
            existing_points[int(record.id)] = (record.payload or {}, record.vector)

        if offset is None:
            break

    return existing_points

def sync_trading_day_against_qdrant(
    client: QdrantClient,
    collection_name: str,
    raw_data_list: List[dict],
    vectors: List[list],
    target_date: date,
):
    # Dung cho truong hop du lieu lich su immutable:
    # 1. Filter tren Qdrant de lay data cua ngay can dong bo
    # 2. Chi upsert nhung record moi hoac record thay doi so voi snapshot cua ngay do
    existing_points = fetch_points_in_qdrant_for_trading_day(
        client=client,
        collection_name=collection_name,
        target_date=target_date,
    )
    changed_raw_data = []
    changed_vectors = []

    for i, (raw_item, vector) in enumerate(zip(raw_data_list, vectors)):
        try:
            point_id = raw_item["point_id"]
            payload_data = {key: value for key, value in raw_item.items() if key != "point_id"}
            validated_payload = StockPayload(**payload_data).model_dump(mode="json")
        except ValidationError as e:
            print(f"❌ Dòng {i} bị lỗi schema, bỏ qua: {e.json()}")
            continue

        if validated_payload["trading_date"] != target_date.isoformat():
            continue

        existing_payload, existing_vector = existing_points.get(point_id, ({}, []))
        if existing_payload == validated_payload and existing_vector == vector:
            continue

        changed_raw_data.append(raw_item)
        changed_vectors.append(vector)

    if not changed_raw_data:
        print(f"Khong co thay doi nao cho ngay {target_date.isoformat()}.")
        return

    batch_upsert_with_validation(
        client=client,
        collection_name=collection_name,
        raw_data_list=changed_raw_data,
        vectors=changed_vectors,
    )

# --- Cách sử dụng ---
raw_data = [
    {"point_id": 101, "ticker": "KBC", "price": 32.1, "trading_date": "2026-05-16"},
    {"point_id": 102, "ticker": "VND", "price": 20.5, "trading_date": "2026-05-16"},
]
vectors = [[0.1]*100, [0.5]*100]
sync_trading_day_against_qdrant(
    client=client,
    collection_name="test_collection",
    raw_data_list=raw_data,
    vectors=vectors,
    target_date=date(2026, 5, 16),
)

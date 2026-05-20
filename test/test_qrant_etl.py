#qrant_extractor
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue


client = QdrantClient(url="http://localhost:6333")

COLLECTION_NAME = 'newspaper'

query_filter = Filter(
    must=[
        FieldCondition(
            key="is_load_to_qdrant",  # Tên trường trong payload
            match=MatchValue(value=0)  # Khớp chính xác giá trị
        ),
    ]
)

# Trường hợp 1: Chỉ lọc dữ liệu thuần túy (không dùng vector search)
response = client.scroll(
    collection_name=COLLECTION_NAME,
    scroll_filter=query_filter,
    limit=10,  # Số lượng kết quả muốn lấy
    with_payload=True,  # Trả về kèm dữ liệu payload
    with_vectors=False  # Không cần trả về chuỗi vector nếu không dùng tới
)

response_ = response[0]
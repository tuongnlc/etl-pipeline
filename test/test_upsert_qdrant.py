from qdrant_client import QdrantClient, models
from qdrant_client import QdrantClient, models

client = QdrantClient(url="http://localhost:6333")

client.delete(
    collection_name="stock_price",
    # Sử dụng Filter trống để match và xóa TOÀN BỘ các points trong collection
    points_selector=models.FilterSelector(
        filter=models.Filter()
    )
)
from qdrant_client import QdrantClient, models
from qdrant_client import QdrantClient, models

client = QdrantClient(url="http://localhost:6333")

client.delete(
    collection_name="stock_price_embedded",
    # Sử dụng Filter trống để match và xóa TOÀN BỘ các points trong collection
    points_selector=models.FilterSelector(
        filter=models.Filter(
            must=[models.FieldCondition(
                    key="trading_date",
                    range=models.DatetimeRange(
                        lt="2026-04-17" # Và ngày nhỏ hơn 2026-04-17
                    )
                )]
        )
    )
)
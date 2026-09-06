from qdrant_client import QdrantClient, models
from qdrant_client import QdrantClient, models

client = QdrantClient(url="http://localhost:6333")

client.delete(
    collection_name="newspaper_embedded",
    # Sử dụng Filter trống để match và xóa TOÀN BỘ các points trong collection
    points_selector=models.FilterSelector(
        filter=models.Filter(
            must=[models.FieldCondition(
                    key="publish_date",
                    range=models.DatetimeRange(
                        gt="2026-09-04" # Và ngày lớn hơn 2026-09-05
                    )
                )]
        )
    )
)
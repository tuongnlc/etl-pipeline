# /Users/tuongnguyen/Desktop/projects/etl-pipeline/src/infrastructure/polars/etl/extract/qdrant_extractor.py
from src.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter


qrant_extractor = QdrantExtractorWithPayloadFilter(
    qrant_url="http://localhost:6333",
    collection_name="newspaper",
    payload_filter={
        "is_load_to_qdrant": 0
    }
)

df = qrant_extractor.extract()
print(df)

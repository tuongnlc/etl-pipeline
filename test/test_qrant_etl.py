# /Users/tuongnguyen/Desktop/projects/etl-pipeline/src/infrastructure/polars/etl/extract/qdrant_extractor.py
from src.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from src.infrastructure.polars.etl.transform.example import ExampleTransformStep
from src.infrastructure.polars.etl.transform.clean_text import CleanTextPolars

extract = QdrantExtractorWithPayloadFilter(
    qrant_url="http://localhost:6333",
    collection_name="newspaper",
    payload_filter={
        "is_load_to_qdrant": 0
    }
)   
df = extract.extract()

transform_steps = [
    ("example", ExampleTransformStep(), (), {}),
    ("clean_text", CleanTextPolars(), ("newspaper_content",), {}),
]

for _, transformer, args, kwargs in transform_steps:
    df = transformer.transform(df, *args, **kwargs)

print(df)


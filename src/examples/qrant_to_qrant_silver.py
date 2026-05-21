import os
from argparse import Namespace


from src.jobs.silver_newspaper import SilverNewspaper


from src.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
from src.infrastructure.polars.etl.transform.clean_text import CleanTextPolars
from src.infrastructure.polars.etl.transform.example import ExampleTransformStep
from src.infrastructure.polars.etl.transform.qdrant_transform import QdrantTransform



extract = QdrantExtractorWithPayloadFilter(
    qrant_url="http://localhost:6333",
    collection_name="newspaper",
    payload_filter={
        "is_load_to_qdrant": 0
    }
)   
df = extract.extract()

# print(df)

transforms = []
transform_step_1 = ExampleTransformStep()
transform_step_2 = CleanTextPolars()

transforms.append(transform_step_1)
transforms.append(transform_step_2)
transformed_steps = transforms

qdrant_transform = QdrantTransform()


silver_newspaper_job = SilverNewspaper(
    extractor=extract,
    transformer=qdrant_transform,
    transform_steps=transformed_steps,
    # loader=loader,
)

silver_newspaper_job.run()

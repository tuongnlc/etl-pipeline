from dataclasses import dataclass
from typing import List
# from etl_pipeline.templates.etl.transform.base import TransformStep
from typing import Any

@dataclass
class QdrantTransformConfig:
    transform_steps: List[Any]

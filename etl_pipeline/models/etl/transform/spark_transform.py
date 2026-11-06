from dataclasses import dataclass
from typing import Any, List


@dataclass
class SparkTransformConfig:
    transform_steps: List[Any]

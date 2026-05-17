from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any

@dataclass
class QdrantLoaderConfig:
    qrant_url: str
    collection_name: str
    qrant_payload: Any

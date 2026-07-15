from dataclasses import dataclass


@dataclass
class QdrantExtractorWithPayloadConfig:
    qdrant_url: str
    collection_name: str
    payload_filter: dict

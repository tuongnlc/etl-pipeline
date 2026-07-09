from dataclasses import dataclass


@dataclass
class QdrantExtractorWithPayloadConfig:
    qrant_url: str
    collection_name: str
    payload_filter: dict

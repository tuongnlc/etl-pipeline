import polars as pl
from etl_pipeline.templates.etl.transform.base import TransformStep
from qdrant_client import QdrantClient


class SelectPayloads(TransformStep):
    """
        Extract data from qdrant database with payload filter

        Parameters:
            qdrant_url (str): Qdrant database URL
            collection_name (str): Collection name to extract data from

        Returns:
            polars.DataFrame: DataFrame containing the extracted data from qdrant database
    """
    def __init__(
        self,
        qdrant_url: str,
        collection_name: str,
        payload_filter: dict,
    ):
        self.qdrant_client = QdrantClient(url=qdrant_url)
        self.collection_name = collection_name
        self.payload_filter = payload_filter


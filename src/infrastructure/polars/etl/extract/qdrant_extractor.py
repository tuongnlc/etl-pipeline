from src.templates.etl.extract.qdrant_extractor import QdrantExtractor
import polars as pl
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue


class QdrantExtractorWithPayloadFilter(QdrantExtractor):
    """
        Extract data from qdrant database with payload filter

        Parameters:
            qrant_url (str): Qdrant database URL
            collection_name (str): Collection name to extract data from

        Returns:
            polars.DataFrame: DataFrame containing the extracted data from qdrant database
    """
    def __init__(
        self,
        qrant_url: str,
        collection_name: str,
        payload_filter: dict,
    ):
        self.qrant_client = QdrantClient(url=qrant_url)
        self.collection_name = collection_name
        self.payload_filter = payload_filter

    def _build_payload_filter(
        self
    ) -> Filter:
        """
            Build payload filter from payload_filter

            Returns:
                Filter: Filter object with payload filter
        """
        must_conditions = []

        for key, value in self.payload_filter.items():
            must_conditions.append(
                FieldCondition(
                    key=key,
                    match=MatchValue(value=value),
                )
            )

        return Filter(must=must_conditions)

    def _extract_with_payload_filter(
        self,
        query_filter: Filter,
    ):
        """
            Extract data from qdrant database with payload filter

            Returns:
                polars.DataFrame: DataFrame containing the extracted data from qdrant database
        """
        records, _ = self.qrant_client.scroll(
            collection_name=self.collection_name,
            scroll_filter=query_filter,
            limit=100,
            with_payload=True,
            with_vectors=False,
        )

        rows = [
            {
                "id": record.id,
                **(record.payload or {}),
            }
            for record in records
        ]

        return pl.DataFrame(rows) if rows else pl.DataFrame()

    def extract(self) -> pl.DataFrame:
        """
            Extract data from qdrant database

            Returns:
                polars.DataFrame: DataFrame containing the extracted data from qdrant database
        """
        query_filter = self._build_payload_filter()
        return self._extract_with_payload_filter(query_filter)
        
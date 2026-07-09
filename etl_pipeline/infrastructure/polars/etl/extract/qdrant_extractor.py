from etl_pipeline.templates.etl.extract.qdrant_extractor import QdrantExtractor
import polars as pl
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue
from typing import Optional


class QdrantExtractorWithPayloadFilter(QdrantExtractor):
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
        batch_size: int = 256,
        max_records: Optional[int] = None,
    ):
        self.qdrant_client = QdrantClient(url=qdrant_url)
        self.collection_name = collection_name
        self.payload_filter = payload_filter
        self.batch_size = batch_size
        self.max_records = max_records

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
        rows: list[dict[str, object]] = []
        next_offset = None

        while True:
            records, next_offset = self.qdrant_client.scroll(
                collection_name=self.collection_name,
                scroll_filter=query_filter,
                with_payload=True,
                with_vectors=False,
                limit=self.batch_size,
                offset=next_offset,
            )

            if not records:
                break

            rows.extend(
                {
                    "id": record.id,
                    **(record.payload or {}),
                }
                for record in records
            )

            if self.max_records is not None and len(rows) >= self.max_records:
                rows = rows[: self.max_records]
                break

            if next_offset is None:
                break

        output_df = pl.DataFrame(rows) if rows else pl.DataFrame()

        print(f"NUMBER of records extracted: {len(rows)}")

        return output_df

    def extract(self) -> pl.DataFrame:
        """
            Extract data from qdrant database

            Returns:
                polars.DataFrame: DataFrame containing the extracted data from qdrant database
        """
        query_filter = self._build_payload_filter()
        print(f"Query to qdrant: {query_filter}")
        return self._extract_with_payload_filter(query_filter)
        

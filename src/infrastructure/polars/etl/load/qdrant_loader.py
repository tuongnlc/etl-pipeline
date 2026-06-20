"""
    Receive data from postgresql database and load to qdrant database
"""
from qdrant_client import QdrantClient
from pydantic import BaseModel
from typing import List
from pydantic import TypeAdapter
import polars as pl
from qdrant_client.models import PointStruct
from src.templates.etl.load.qdrant_loader import QdrantLoader
from qdrant_client.models import Filter
from qdrant_client.models import Filter, FieldCondition, MatchValue

class QdrantLoader(QdrantLoader):
    """
        Load arrow table to qdrant database
        Use Qrantclient cause polars don't support qdrant now

        Input:
            pyarrow.Table
        Output:
            None
    """
    def __init__(self, 
                qrant_url: str,
                destination_collection_name: str,
                qrant_payload: type[BaseModel],
                is_upsert_source_table: bool = False,
                source_name: str | None = None, 
                qrant_payload_for_source_table: dict | None = None,
                payload_filter_for_source_table: dict | None = None,
            ) -> None:
        self.qdrant_client = QdrantClient(url=qrant_url)
        self.destination_collection_name = destination_collection_name
        self.qrant_payload = qrant_payload
        self.source_name = source_name
        self.is_upsert_source_table = is_upsert_source_table
        self.qrant_payload_for_source_table = qrant_payload_for_source_table
        self.payload_filter_for_source_table = payload_filter_for_source_table


    def _build_payload_filter( #Duplicate code with bronze_layer. Update latẻ
        self,
        payload_filter: dict,
    ) -> Filter:
        """
            Build payload filter from payload_filter

            Returns:
                Filter: Filter object with payload filter
        """
        must_conditions = []

        for key, value in payload_filter.items():
            must_conditions.append(
                FieldCondition(
                    key=key,
                    match=MatchValue(value=value),
                )
            )

        return Filter(must=must_conditions)
        
    def _valid_schema(self, raw_data_list: List[dict]):
        """
            Validate schema of qrant payload
        """ 
        payload_list_adapter = TypeAdapter(List[self.qrant_payload])
        try:
            validated_payloads = payload_list_adapter.validate_python(raw_data_list)
        except Exception as e:
            raise ValueError(f"Payload validation failed: {e}") from e
        
        return validated_payloads
    
    def _payload_to_dict(self, payload: BaseModel) -> dict:
        if hasattr(payload, "model_dump"):
            return payload.model_dump(mode="json")
        return payload.dict()

    def load(self, records: pl.DataFrame, vector_column: str | None = "chunk_embedded"):
        """
            Load arrow table to qdrant database
        """
        if records.height == 0:
            return

        print("NUMBER of records to load:")
        print(len(records))

        #update here to back fill to not reach quota
        records = records.head(50)

        raw_data_list = records.to_dicts()

        if not raw_data_list:
            return
        
        #Check schema of qrant payload
        validated_payloads = self._valid_schema(raw_data_list)

        # #Load data to qdrant database
        if vector_column is not None and vector_column not in records.columns:
            raise ValueError(
                f"Missing vector column '{vector_column}'. Available columns: {records.columns}. "
                "Pass vector_column=None to upsert payload-only points, or add an embedding transform step."
            )

        points = []
        for item, payload in zip(raw_data_list, validated_payloads):
            payload_dict = self._payload_to_dict(payload)
            if vector_column is None:
                points.append(
                    PointStruct(
                        id=item["id"],
                        vector={},
                        payload=payload_dict,
                    )
                )
            else:
                points.append(
                    PointStruct(
                        id=item["id"],
                        vector=item[vector_column],
                        payload=payload_dict,
                    )
                )

        self.qdrant_client.upload_points(
            collection_name=self.destination_collection_name,
            points=points,
            wait=False, # Set False để tăng tốc độ nếu không cần đọc ngay lập tức
            batch_size=10,
        )

        #upsert source table
        if self.is_upsert_source_table:
            if not self.source_name:
                raise ValueError("source_name must be provided when is_upsert_source_table is True")
            if self.qrant_payload_for_source_table is None:
                raise ValueError(
                    "qrant_payload_for_source_table must be provided when is_upsert_source_table is True"
                )

            payload_filter_for_source_table = self.payload_filter_for_source_table
            qdrant_filter = self._build_payload_filter(payload_filter_for_source_table)
            self.qdrant_client.set_payload(
                collection_name=self.source_name,
                payload=self.qrant_payload_for_source_table,
                points=qdrant_filter,
                wait=True,
            )

"""
    Receive data from postgresql database and load to qdrant database
"""
from qdrant_client import QdrantClient
from pydantic import BaseModel
import polars as pl
import uuid
from qdrant_client.models import PointStruct, SparseVector
from etl_pipeline.templates.etl.load.qdrant_loader import QdrantLoader
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
                qdrant_url: str,
                destination_collection_name: str,
                is_upsert_source_table: bool = False,
                source_name: str | None = None, 
                qdrant_payload_for_source_table: dict | None = None,
                payload_filter_for_source_table: dict | None = None,
            ) -> None:
        self.qdrant_client = QdrantClient(url=qdrant_url)
        self.destination_collection_name = destination_collection_name
        self.source_name = source_name
        self.is_upsert_source_table = is_upsert_source_table
        self.qdrant_payload_for_source_table = qdrant_payload_for_source_table
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
        
    # def _valid_schema(self, raw_data_list: List[dict]):
    #     """
    #         Validate schema of qrant payload
    #     """ 
    #     payload_list_adapter = TypeAdapter(List[self.qrant_payload])
    #     try:
    #         validated_payloads = payload_list_adapter.validate_python(raw_data_list)
    #     except Exception as e:
    #         raise ValueError(f"Payload validation failed: {e}") from e
        
        # return validated_payloads
    
    def _payload_to_dict(self, payload: BaseModel) -> dict:
        if hasattr(payload, "model_dump"):
            return payload.model_dump(mode="json")
        return payload.dict()

    def _normalize_uuid_like_value(self, value):
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, memoryview):
            value = value.tobytes()
        if isinstance(value, (bytes, bytearray)) and len(value) == 16:
            return str(uuid.UUID(bytes=bytes(value)))
        return value

    def load(self, 
            records: pl.DataFrame, 
            dense_vector_column: str | None = None, 
            sparse_vector_indices_column: str | None = None,
            sparse_vector_values_column: str | None = None,
        ):
        """
            Load arrow table to qdrant database
        """
        if records.height == 0:
            return

        required_columns = {"id"}
        optional_vector_columns = {
            dense_vector_column,
            sparse_vector_indices_column,
            sparse_vector_values_column,
        }
        required_columns.update(
            column_name for column_name in optional_vector_columns if column_name is not None
        )

        missing_columns = sorted(required_columns.difference(records.columns))
        if missing_columns:
            raise ValueError(
                "Missing required columns for Qdrant load: "
                f"{missing_columns}. Available columns: {records.columns}"
            )

        print("NUMBER of vector to write to qdrant:")
        print(len(records))

        dense_vector_name = None
        sparse_vector_name = None
        if sparse_vector_indices_column is not None and sparse_vector_values_column is not None:
            collection_info = self.qdrant_client.get_collection(
                self.destination_collection_name
            )
            dense_vector_name = (
                list(collection_info.config.params.vectors.keys())[0]
                if isinstance(collection_info.config.params.vectors, dict)
                else ""
            )
            sparse_vector_name = list(
                collection_info.config.params.sparse_vectors.keys()
            )[0]

        points = []
        
        for item in records.to_dicts():
            item = {
                key: self._normalize_uuid_like_value(value)
                for key, value in item.items()
            }
            payload_dict = {
                    key: value
                    for key, value in item.items()
                    if key != "id" and key != dense_vector_column and key != sparse_vector_indices_column and key != sparse_vector_values_column
                }

            if (
                dense_vector_column is None
                and sparse_vector_indices_column is None
                and sparse_vector_values_column is None
            ):
                points.append(
                    PointStruct(
                        id=item["id"],
                        vector={},
                        payload=payload_dict,
                    )
                )
            elif sparse_vector_indices_column is None and sparse_vector_values_column is None and dense_vector_column is not None: 
                points.append(
                    PointStruct(
                        id=item["id"],
                        vector=item[dense_vector_column],
                        payload=payload_dict,
                    )
                )
            elif sparse_vector_indices_column is not None and sparse_vector_values_column is not None:
                points.append(
                    PointStruct(
                        id=item["id"],
                        vector={
                            dense_vector_name: item[dense_vector_column],
                            sparse_vector_name: SparseVector(
                                indices=item[sparse_vector_indices_column],
                                values=item[sparse_vector_values_column],
                            ),
                        },
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
            if self.qdrant_payload_for_source_table is None:
                raise ValueError(
                    "qdrant_payload_for_source_table must be provided when is_upsert_source_table is True"
                )

            payload_filter_for_source_table = self.payload_filter_for_source_table
            qdrant_filter = self._build_payload_filter(payload_filter_for_source_table)
            self.qdrant_client.set_payload(
                collection_name=self.source_name,
                payload=self.qdrant_payload_for_source_table,
                points=qdrant_filter,
                wait=True,
            )

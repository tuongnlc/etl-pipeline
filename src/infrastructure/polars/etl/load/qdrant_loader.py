"""
    Receive data from postgresql database and load to qdrant database
"""
from qdrant_client import QdrantClient
from pydantic import BaseModel
from typing import List
from pydantic import TypeAdapter
import pyarrow as pa
from qdrant_client.models import PointStruct
from src.templates.etl.load.qdrant_loader import QdrantLoader

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
                collection_name: str,
                qrant_payload: BaseModel
            ) -> None:
        self.qdrant_client = QdrantClient(url=qrant_url)
        self.collection_name = collection_name
        self.qrant_payload = qrant_payload

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

    def load(self, records: pa.Table):
        """
            Load arrow table to qdrant database
        """
        #Convert arrow table to python list
        raw_data_list = records.to_pylist()
        print(raw_data_list)
        
        #Check schema of qrant payload
        validated_payloads = self._valid_schema(raw_data_list)

        # #Load data to qdrant database
        points = [
            PointStruct(
                id=item["id"],
                vector={},
                payload=payload.model_dump(mode="json"),
            )
            for item, payload in zip(raw_data_list, validated_payloads)
        ]

        self.qdrant_client.upload_points(
            collection_name=self.collection_name,
            points=points,
            wait=False, # Set False để tăng tốc độ nếu không cần đọc ngay lập tức
            batch_size=10,
        )
        
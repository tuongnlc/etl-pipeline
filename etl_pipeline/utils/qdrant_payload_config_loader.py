from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, create_model

TYPE_MAP = {
    "str": str,
    "int": int,
    "date": date,
    "float": float,
    "bool": bool,
    "datetime": datetime,
    "Optional[date]": Optional[date],
    "Optional[datetime]": Optional[datetime],
    "Optional[float]": Optional[float],
    "Optional[bool]": Optional[bool],
}

def build_payload_model(model_name: str, payload_config: dict) -> type[BaseModel]:
    fields = {}

    for field_name, field_type_name in payload_config.items():
        if field_type_name not in TYPE_MAP:
            raise ValueError(f"Unsupported qrant_payload type: {field_type_name}")

        field_type = TYPE_MAP[field_type_name]
        default_value = None if field_type_name.startswith("Optional[") else ...
        fields[field_name] = (field_type, default_value)

    return create_model(model_name, **fields)
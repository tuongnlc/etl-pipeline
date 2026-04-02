import re
from typing import Any
from pydantic import BaseModel, field_validator


class TaskFactoryConfig(BaseModel):
    """
        Configure for a single task factory instance

        Attributes:
            id (str): Unique identifier for the task factory
            factory_type (str): Type of the task factory to use
            dependencies (list[str]): List of tasks id this task depends on
            args (dict[str, Any]): Arguments to pass to the task factory
    """
    id: str
    factory_type: str
    dependencies: list[str] = []
    args: dict[str, Any] = {}

    @field_validator("id")
    def validate_id(cls, v: str) -> str:
        """
            Validate id is in right format
        """
        if not re.match(r"^[a-zA-Z0-9_]+$", v):
            raise ValueError(f"Invalid id: {v}")
        return v
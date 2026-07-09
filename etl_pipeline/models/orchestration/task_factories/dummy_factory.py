from typing import Any, Optional, ClassVar
from pydantic import BaseModel, ConfigDict


class DummyTaskModel(BaseModel):
    """
    Model validation cho dummy task.
    """
    valid_args: ClassVar[list[str]] = ["custom_dummy_arg1", "custom_dummy_arg2", "job_config", "cluster_type"]
    model_config = ConfigDict(extra='allow')
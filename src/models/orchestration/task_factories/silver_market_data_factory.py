from typing import Any, Optional, ClassVar
from pydantic import BaseModel, ConfigDict


class SilverMarketDataTaskModel(BaseModel):
    """
    Model validation cho silver market data task.
    """
    valid_args: ClassVar[list[str]] = ["task_id", "job_config_path", "enable_delete_before_load"]
    model_config = ConfigDict(extra='allow')

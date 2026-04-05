from typing import Any, Optional, ClassVar
from pydantic import BaseModel, ConfigDict


class SilverMarketDataTaskModel(BaseModel):
    """
    Model validation cho silver market data task.
    """
    valid_args: ClassVar[list[str]] = ["job_config", "cluster_type"]
    model_config = ConfigDict(extra='allow')
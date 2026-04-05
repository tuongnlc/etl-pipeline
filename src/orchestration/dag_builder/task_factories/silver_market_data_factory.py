import logging
from src.models.orchestration.task_factories.task_factory import TaskFactoryBase
from typing import Any
from airflow.sdk import TaskGroup
from airflow.sdk import Asset
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)


class SilverMarketDataTaskFactory(TaskFactoryBase):
    """
        Silver market data task factory.
    """
    def validate_args(self, args: dict[str, Any]) -> None:
        """
            Validate the arguments passed to the task factory.
        """
        if "job_config_path" not in args:
            raise ValueError("job_config_path is required")

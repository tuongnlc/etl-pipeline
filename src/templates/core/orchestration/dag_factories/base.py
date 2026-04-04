from abc import ABC, abstractmethod
from typing import Any
from src.models.orchestration.airflow_dag import DagDefinition
from airflow.sdk import DAG
from src.models.orchestration.airflow_dag import DagDefinitionSpec
from src.models.orchestration.airflow_dag import DagDefaultArgs
from src.models.orchestration.airflow_dag import TaskFactoryConfig



class DagFactoryBase(ABC):
    """
        Base class for DAG factory.
    """
    @abstractmethod
    def create_dag(self, dag_definition: DagDefinition) -> DAG:
        """
            Create a DAG with give DAG definition
        """
        pass

    @abstractmethod
    def _create_dag_instance(self, spec: DagDefinitionSpec) -> DAG:
        """
            Create DAG instance from spec
        """
        pass

    @abstractmethod
    def _build_default_args(self, default_args_spec: DagDefaultArgs) -> dict[str, Any]:
        """
            Build default args from spec
        """
        pass

    @abstractmethod
    def _create_tasks(self, dag: DAG, task_configs: list[TaskFactoryConfig]) -> dict[str, Any]:
        """
            Create tasks for spec
        """
        pass

    @abstractmethod
    def _setup_dependencies(self, task_map: dict[str, Any], task_configs: list[TaskFactoryConfig]) -> None:
        """
            Setup dependencies for tasks
        """
        pass
    
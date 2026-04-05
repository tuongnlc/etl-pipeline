from abc import ABC, abstractmethod
from typing import Any

from airflow.sdk import DAG
from airflow.sdk import TaskGroup


class TaskFactoryBase(ABC):
    """
        Base class for task factory.

        Input:
            task_group_id: The ID of the task group to create.
            dag: The DAG to create the task group for.
            args: The arguments to use to create the task group.

        Output:
            The created TaskGroup.
    """
    def create_task(self, task_group_id: str, dag: DAG, args: dict[str, Any]) -> TaskGroup:
        """
            Create a task group for the given DAG
        """
        self.validate_args(args)
        return self._create_task_impl(task_group_id, dag, args)
        
    @abstractmethod
    def validate_args(self, args: dict[str, Any]) -> None:
        """
            Validate the given arguments
        """
        ...

    @abstractmethod
    def _create_task_impl(
        self, task_group_id: str, dag: DAG, args: dict[str, Any]
    ) -> TaskGroup:
        """
            Create a task group for the given task group ID
        """
        ...

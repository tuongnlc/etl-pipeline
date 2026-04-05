from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional
import logging

from airflow.sdk import DAG
from airflow.sdk import TaskGroup


logger = logging.getLogger(__name__)


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
    
    # Class variable to be overridden by subclasses
    model_class: Optional[type] = None
    
    def create_task(self, task_group_id: str, dag: DAG, args: dict[str, Any]) -> TaskGroup:
        """
            Create a task group for the given DAG
        """
        self.validate_args(args)
        return self._create_task_impl(task_group_id, dag, args)
        
    def validate_args(self, args: dict[str, Any]) -> None:
        """
            Validate the arguments passed to the task factory using the model class.
            This method provides generic validation that can be reused by subclasses.
        """
        if self.model_class:
            # Pydantic validation
            model = self.model_class(**args) 
            
            # Check for unknown arguments - if user put unknown arguments, raise error
            if hasattr(self.model_class, 'valid_args'):
                unknown_args = set(args.keys()) - set(self.model_class.valid_args)
                if unknown_args:
                    logger.error(f"Unknown arguments: {unknown_args}")
                    raise ValueError(f"Unknown arguments: {unknown_args}")
        else:
            # Fallback to abstract method if no model_class is defined
            self._validate_args_impl(args)
    
    def _validate_args_impl(self, args: dict[str, Any]) -> None:
        """
            Validate the given arguments (abstract implementation)
            Subclasses should override this if they don't use model_class
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

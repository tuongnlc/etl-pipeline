from abc import ABC, abstractmethod
from typing import Any


class TaskFactoryBase(ABC):
    @abstractmethod
    def create_tasks(self, task_group_id: str, dag, args: Any) -> Any:
        pass

    @abstractmethod
    def validate_args(self, args: Any) -> None:
        pass

    @abstractmethod
    def _create_task_impl(self, task_group_id: str, dag, args: Any) -> Any:
        pass

class TaskFactoryRegistry:
    def __init__(self):
        self._factories: dict[str, TaskFactoryBase] = {}

    def register(self):
        pass

    def get(self):
        pass

    def exits(self):
        pass

    def list_factories(self):
        return self._factories.keys()

    def clear(self):
        self._factories.clear()
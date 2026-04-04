from models.orchestration.task_factory import TaskFactoryBase


class TaskFactoryRegistry:
    """
        Registry for task. It contains multiple task factories.
    """
    def __init__(self):
        self._factories: dict[str, TaskFactoryBase] = {}

    def register(self, factory_type: str, factory: TaskFactoryBase) -> TaskFactoryBase:
        """
            Register a task factory with the registry.
        """
        if factory_type not in self._factories:
            return
        return self._factories[factory_type]

    def get(self, factory_type: str) -> TaskFactoryBase:
        """
            Get a task factory from the registry.
        """
        if factory_type not in self._factories:
            raise ValueError(f"Factory type {factory_type} is not registered. Available types: {', '.join(sorted(self._factories.keys()))}")
        return self._factories[factory_type]

    def exists(self, factory_type: str) -> bool:
        """
            Check if a task factory is registered with the given type.
        """
        return factory_type in self._factories

    def list_factories(self) -> list[str]:
        """
            List all the registered task factory types.
        """
        return sorted(self._factories.keys())

    def clear(self) -> None:
        """
            Clear all the registered task factories.
        """
        self._factories.clear()
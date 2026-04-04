from src.utils.airflow_config_loader import load_config
from typing import Any
from src.orchestration.config.config_registry import ConfigRegistry
# from src.orchestration.task_factories.task_factory_registry import TaskFactoryRegistry
# from src.orchestration.task_factories.dummy_factory import DummyTaskFactory



configs_path = "local://src/configs/orchestration"
raw_configs: list[dict[str, Any]] = load_config(path=configs_path)
config_registry = ConfigRegistry()
config_registry.populate(raw_configs)

task_factory_registry: TaskFactoryRegistry = TaskFactoryRegistry()
task_factory_registry.register(DummyTaskFactory())

print(task_factory_registry.__dict__)
print(task_factory_registry.list_factories())


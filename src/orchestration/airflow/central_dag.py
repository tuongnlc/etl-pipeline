from src.utils.airflow_config_loader import load_config
from typing import Any
from src.orchestration.config.config_registry import ConfigRegistry
from src.orchestration.dag_builder.task_factories.task_factory_registry import TaskFactoryRegistry
from src.orchestration.dag_builder.task_factories.dummy_factory import DummyTaskFactory
from src.orchestration.dag_builder.dag_builder import DagBuilder



configs_path = "local://src/configs/orchestration"
raw_configs: list[dict[str, Any]] = load_config(path=configs_path)
config_registry = ConfigRegistry()
config_registry.populate(raw_configs)

task_factory_registry: TaskFactoryRegistry = TaskFactoryRegistry()
task_factory_registry.register(DummyTaskFactory())

dag_builder = DagBuilder(config_registry=config_registry, task_factory_registry=task_factory_registry)

try:
    all_dags = dag_builder.build_all()
except Exception as e:
    raise e

for dag_id, dag_obj in all_dags.items():
    globals()[dag_id] = dag_obj

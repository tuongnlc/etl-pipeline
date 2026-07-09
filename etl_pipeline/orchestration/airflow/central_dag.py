from etl_pipeline.utils.airflow_config_loader import load_config
from typing import Any
from pathlib import Path
from etl_pipeline.utils import airflow_config_loader
from etl_pipeline.orchestration.config.config_registry import ConfigRegistry
from etl_pipeline.orchestration.dag_builder.task_factories.task_factory_registry import TaskFactoryRegistry
from etl_pipeline.orchestration.dag_builder.task_factories.dummy_factory import DummyTaskFactory
from etl_pipeline.orchestration.dag_builder.dag_builder import DagBuilder
from etl_pipeline.orchestration.dag_builder.task_factories.silver_market_data_factory import SilverMarketDataTaskFactory
from etl_pipeline.orchestration.dag_builder.task_factories.bronze_newspaper_factory import BronzeNewspaperTaskFactory
from etl_pipeline.orchestration.dag_builder.task_factories.silver_newspaper_factory import SilverNewspaperTaskFactory
from etl_pipeline.orchestration.dag_builder.task_factories.trigger_dag_factory import TriggerDagsFactory



configs_path = f"local://{Path(airflow_config_loader.__file__).resolve().parents[1] / 'configs' / 'orchestration'}"
raw_configs: list[dict[str, Any]] = load_config(path=configs_path)
config_registry = ConfigRegistry()
config_registry.populate(raw_configs)

task_factory_registry: TaskFactoryRegistry = TaskFactoryRegistry()
task_factory_registry.register(DummyTaskFactory())
task_factory_registry.register(SilverMarketDataTaskFactory())
task_factory_registry.register(BronzeNewspaperTaskFactory())
task_factory_registry.register(SilverNewspaperTaskFactory())
task_factory_registry.register(TriggerDagsFactory())



dag_builder = DagBuilder(config_registry=config_registry, task_factory_registry=task_factory_registry)

try:
    all_dags = dag_builder.build_all()
except Exception as e:
    raise e

for dag_id, dag_obj in all_dags.items():
    globals()[dag_id] = dag_obj

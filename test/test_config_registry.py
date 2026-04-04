from src.utils.airflow_config_loader import load_config
from typing import Any
from src.orchestration.airflow.core.config import ConfigRegistry



configs_path = "local://src/configs/orchestration"
raw_configs: list[dict[str, Any]] = load_config(path=configs_path)
config_registry = ConfigRegistry()
config_registry.populate(raw_configs)

print(config_registry)
print(config_registry.configs.get("example_2")) # --> Return DagDefinitionObject
print(config_registry.configs.get("example_2").spec.start_date) # --> Return date object
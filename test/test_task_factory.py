from src.utils.airflow_config_loader import load_config
from typing import Any
from src.orchestration.config.config_registry import ConfigRegistry


configs_path = "local://src/configs/orchestration"
raw_configs: list[dict[str, Any]] = load_config(path=configs_path)
config_registry = ConfigRegistry()
config_registry.populate(raw_configs)

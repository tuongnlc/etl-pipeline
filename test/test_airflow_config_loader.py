from src.utils.airflow_config_loader import load_config
from typing import Any



configs_path = "local://src/configs/orchestration"
raw_configs: list[dict[str, Any]] = load_config(path=configs_path)
print(raw_configs)

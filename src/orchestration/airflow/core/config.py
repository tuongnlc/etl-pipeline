from typing import Type
from src.models.orchestration.base_config import BaseConfig
# from daglib.models.airflow import DagDefinition
from src.models.orchestration.airflow_dag import DagDefinition
from typing import Any
import logging
from typing import Literal, Sequence
from src.models.orchestration.task_factory import TaskFactoryConfig


MODEL_MAPPING: dict[str, Type[BaseConfig]] = {
    "DagDefinition": DagDefinition,
}


class ConfigRegistry:
    """
        Config registry to store and manage all config objects

        Steps:
            1. Load raw config data from file
            2. Go through each config and transform it to model object base on kind
            3. Register model object to registry 

        Output:
            Config registry with all config objects saving in configs
    """
    def __init__(self):
        self.configs: dict[str, BaseConfig] = {}

    def populate(self, raw_configs: list[dict[str, Any]]) -> None:
        """
            Populate config registry with raw config data
        """
        for data in raw_configs:
            kind: Any | None = data.get("kind")
            if not kind:
                continue
            config_name = (data.get("metadata") or {}).get("name", "<unknown>")

            try:
                model_class: type[BaseConfig] = MODEL_MAPPING[kind]
                config_model: BaseConfig = model_class.model_validate(obj=data)
                name: str = config_model.metadata.name
                if name in self.configs:
                    raise ValueError(f"Duplicate config name: {name}")
                logging.info(f"Populate config: {name}")
                self.configs[name] = config_model # Store config object in registry
            except KeyError:
                logging.warning(f"Invalid config kind: {kind}")
            except Exception as e:
                logging.error(f"Error populating config: {config_name}, error: {e}")

    # @overload # Update here
    # def get_all_config_by_kind(
    #     self, kind: Literal["DagDefinition"]
    # ) -> Sequence[DagDefinition]: ...

    # @overload 
    # def get_all_config_by_kind(
    #     self, kind: Literal["TaskFactoryConfig"]
    # ) -> Sequence[TaskFactoryConfig]: ...

    def get_all_config_by_kind(
        self, kind: Literal["TaskFactoryConfig"]
    ) -> Sequence[TaskFactoryConfig]: ...

    def get_all_config_by_kind(self, kind: str) -> Sequence[BaseConfig]:
        """
            Get all config with given kind
        """
        return [config for config in self.configs.values() if config.kind == kind]

    def get_model_for_kind(self, kind: str) -> type[BaseConfig]:
        """
            Get model class for given kind
        """
        model: type[BaseConfig] = MODEL_MAPPING.get(kind, None)
        
        if not model:
            raise ValueError(f"Invalid config kind: {kind}")
        return model

    def register(self, config_obj: BaseConfig) -> None:
        """
            Register config object to registry
        """
        name: str = config_obj.metadata.name
        if name in self.configs:
            raise ValueError(f"Duplicate config name: {name}")
        logging.info(f"Register config: {name}")
        self.configs[name] = config_obj

    def get_by_name(self, name: str) -> BaseConfig:
        if name not in self.configs:
            raise ValueError(f"Config not found: {name}")
        return self.configs[name]
       

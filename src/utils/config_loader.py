from argparse import Namespace
from pathlib import Path
from jinja2 import Template
from datetime import timedelta, datetime
import yaml
from typing import Any
from dataclasses import is_dataclass
import importlib
import inspect
from src.models.example_config import ExampleConfig
from src.models.etl.extractor.postgres_extractor_with_polars import PostgreDBExtractorWithPolarsConfig
from src.models.etl.loader.bq_loader_with_polars import BigQueryLoaderPolarsConfig
from src.models.etl.jobs.postgre_to_bq_silver import PostgreToBqSilverConfig
from src.models.etl.jobs.postgre_to_qdrant_bronze import PostgreToQdrantBronzeConfig
from src.models.etl.jobs.qdrant_to_qdrant_silver import QdrantToQdrantSilverConfig
from src.models.etl.loader.qdrant_loader import QdrantLoaderConfig
from dacite import from_dict
from src.models.etl.transform.qdrant_transform import QdrantTransformConfig
from src.models.etl.extractor.qdrant_extractor_with_payload import QdrantExtractorWithPayloadConfig
from src.models.etl.jobs.trigger_dag import TriggerDagJobConfig



CONFIG_PARSER_MAP = {
    ExampleConfig.__name__: ExampleConfig,
    PostgreDBExtractorWithPolarsConfig.__name__: PostgreDBExtractorWithPolarsConfig,
    BigQueryLoaderPolarsConfig.__name__: BigQueryLoaderPolarsConfig,    
    PostgreToBqSilverConfig.__name__: PostgreToBqSilverConfig,
    PostgreToQdrantBronzeConfig.__name__: PostgreToQdrantBronzeConfig,
    QdrantLoaderConfig.__name__: QdrantLoaderConfig,
    QdrantToQdrantSilverConfig.__name__: QdrantToQdrantSilverConfig,
    QdrantExtractorWithPayloadConfig.__name__: QdrantExtractorWithPayloadConfig,
    QdrantTransformConfig.__name__: QdrantTransformConfig,
    TriggerDagJobConfig.__name__: TriggerDagJobConfig,
}



def _import_class_from_path(kind: str) -> type[Any]:
    module_path, _, class_name = kind.rpartition(".")
    if not module_path or not class_name:
        raise ValueError(f"Invalid class path: {kind}")

    module = importlib.import_module(module_path)
    cls = getattr(module, class_name, None)
    if cls is None or not inspect.isclass(cls):
        raise ValueError(f"Class not found: {kind}")
    return cls

def load_and_parse_config(
    config_path: str,
    runtime_args: Namespace #Update here
):
    """
        Load and parse config from path for etl process
    """
    config_str = load_yaml_config_from_path_as_str(config_path) #RETURN CONFIG STRING

    jinja_template = Template(config_str) # Use when we need to import library and allow jinja template do parser
    allow_jinja_context = {
        "runtime_args": runtime_args,
        "timedelta": timedelta,
        "str": str,
        "datetime": datetime,
        # "execution_date": runtime_args.execution_date
    }
    rendered_config = jinja_template.render(allow_jinja_context) #string type

    config_dict = yaml.safe_load(rendered_config)
    parse_render_config = parse_config(config_dict)
    return parse_render_config

def parse_config(config_dict: dict[str, Any]) -> Any:
    if not isinstance(config_dict, dict):
        raise ValueError(f"Config must be a dict, got: {type(config_dict)}")

    if "kind" not in config_dict:
        raise ValueError("Config kind is required.")
        
    kind = config_dict["kind"]

    config_class = CONFIG_PARSER_MAP.get(kind)
    if config_class is None and "." in kind:
        config_class = _import_class_from_path(kind)

    if config_class is None:
        raise ValueError(f"Unknown config kind: {kind}")

    spec = config_dict.get("spec") or {}

    if is_dataclass(config_class):
        return from_dict(data_class=config_class, data=spec)

    if isinstance(spec, dict):
        try:
            return config_class(**spec)
        except TypeError:
            return config_class()

    return config_class(spec)

def load_yaml_config_from_path_as_str(path: str) -> str:
    """
        Read content from config file path as string.
    """
    config_path = Path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            file_content = f.read()
            return file_content
    except Exception as e:
        raise e

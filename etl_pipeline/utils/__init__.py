from argparse import Namespace
from pathlib import Path
from jinja2 import Template
from datetime import timedelta, datetime
import yaml
from typing import Any
# from spark.configlib.parser.delta import DeltaTableConfig
# from spark.configlib.parser.example import ExampleConfig
from dacite import from_dict


# CONFIG_PARSER_MAP = {
#     DeltaTableConfig.__name__: DeltaTableConfig,
#     ExampleConfig.__name__: ExampleConfig,
# }

def load_and_parse_config(
    config_path: str,
    # runtime_args: Namespace
):
    config_str = load_yaml_config_from_path_as_str(config_path) #RETURN CONFIG STRING

    jinja_template = Template(config_str) # Use when we need to import library and allow jinja template do parser
    allow_jinja_context = {
        # "runtime_args": runtime_args,
        "timedelta": timedelta,
        "str": str,
        "datetime": datetime,
    }
    rendered_config = jinja_template.render(allow_jinja_context) #string type

    config_dict = yaml.safe_load(rendered_config)
    parse_render_config = parse_config(config_dict)
    return parse_render_config

def parse_config(config_dict: str) -> Any:
    if "kind" not in config_dict:
        raise ValueError("Config kind is required.")
        
    if config_dict["kind"] not in CONFIG_PARSER_MAP:
        raise ValueError(f"Unknown config kind: {config_dict['kind']}")
    
    # Get type of config class
    config_class = CONFIG_PARSER_MAP[config_dict["kind"]]

    # Convert dict to data_class
    parsed_config = from_dict(data_class=config_class, data=config_dict["spec"])
    return parsed_config

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
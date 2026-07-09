import json
from typing import Optional
from argparse import Namespace
from argparse import ArgumentParser
from etl_pipeline.utils.config_loader import load_and_parse_config, parse_config


def etl_job_args_utils(extra_args: Optional[list[dict]] = None) -> Namespace:
    """
        Parse job arguments then transfer config to code
    """
    parser = ArgumentParser()
    parser.add_argument(
        "--job_config", 
        help="Path to job config file.",
        default="",
        type=str, 
        required=True
    )

    args = parser.parse_args()

    if args.job_config != "": #Note here: args.job_config is path of yaml file
        parsed_job_config = load_and_parse_config(args.job_config, args) # RETURN CONFIG OBJECT
        args.job_config = parsed_job_config

        if args.job_config.extractor is not None:
            parsed_job_config_extractpr = parse_config(args.job_config.extractor)
            args.job_config.extractor = parsed_job_config_extractpr
        if args.job_config.loader is not None:
            parsed_job_config_loader = parse_config(args.job_config.loader)
            args.job_config.loader = parsed_job_config_loader
            

    # if args.meta_config != "": Update here
        # parsed_meta_config = load_and_parse_config(args.meta_config, args)
        # args.meta_config = parsed_meta_config
    return args

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
    parser.add_argument(
        "--execution_date",
        help=(
            "Ngày chạy job (YYYY-MM-DD), dùng cho backfill. "
            "Nếu bỏ trống sẽ mặc định là ngày hiện tại."
        ),
        default=None,
        type=str,
        required=False,
    )

    args = parser.parse_args()

    if args.job_config != "": #Note here: args.job_config is path of yaml file
        parsed_job_config = load_and_parse_config(args.job_config, args) # RETURN CONFIG OBJECT
        args.job_config = parsed_job_config

        # Chỉ parse các field config mà job hiện tại có (một số job như
        # datalake_to_datamart chỉ có transform, không có extractor/loader).
        # Job crawl dùng field `crawler`; các job khác dùng `extractor`.
        for field_name in ("extractor", "crawler"):
            if getattr(args.job_config, field_name, None) is not None:
                setattr(
                    args.job_config,
                    field_name,
                    parse_config(getattr(args.job_config, field_name)),
                )
        if getattr(args.job_config, "loader", None) is not None:
            parsed_job_config_loader = parse_config(args.job_config.loader)
            args.job_config.loader = parsed_job_config_loader
        if getattr(args.job_config, "transform", None) is not None:
            parsed_job_config_transform = parse_config(args.job_config.transform)
            args.job_config.transform = parsed_job_config_transform
            # Parse từng transform step (kind/spec) thành object.
            if getattr(parsed_job_config_transform, "transform_steps", None) is not None:
                parsed_job_config_transform.transform_steps = [
                    parse_config(step) for step in parsed_job_config_transform.transform_steps
                ]
            

    # if args.meta_config != "": Update here
        # parsed_meta_config = load_and_parse_config(args.meta_config, args)
        # args.meta_config = parsed_meta_config
    return args

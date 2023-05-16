import json
import logging
import os
from functools import lru_cache
from glob import glob
from pathlib import Path
from typing import Dict, List, Mapping, Union

from sh import dbt


@lru_cache
def get_environment() -> str:
    if os.getenv("GITHUB_TOKEN"):
        env = "prod"
    else:
        env = "dev"

    logging.info(f"Running on environment: {env}")

    return env


@lru_cache
def get_output_dir() -> str:
    if get_environment() == "prod":
        dir = "./output/landing_zone"
    else:
        dir = "./local_output/landing_zone"

    return dir


@lru_cache
def get_json_content(domain: str) -> List[Mapping[str, Union[str, int]]]:
    contents = []
    for file_name in glob(f"{get_output_dir()}/domain={domain}/*/*/*.json"):
        with open(file_name) as f:
            d = json.loads(f.read())
            for x in d:
                x["file_name"] = file_name
        contents += d

    logging.info(f"Read {len(contents)} blogs from json files")
    return contents


def run_dbt_commands(commands: List[str]) -> None:
    for command in commands:
        command_fmt = command.split(" ")[1:] + [
            "--profiles-dir",
            "./dbt",
            "--project-dir",
            "./dbt",
            "--target",
            get_environment(),
        ]
        logging.info(f"Running dbt command: {command_fmt}")
        print(f"Running dbt command: {command_fmt}")
        dbt(command_fmt, _fg=True)


def save_to_landing_zone(data: List[Dict[str, object]], file_name: str) -> None:
    file_name = f"{get_output_dir()}/{file_name}"
    logging.info(f"Saving {len(data)} entries to {file_name}...")

    Path(file_name[: file_name.rfind("/")]).mkdir(parents=True, exist_ok=True)
    with open(file_name, "w", encoding="utf-8") as f_write:
        json.dump(
            data,
            f_write,
            ensure_ascii=False,
            indent=4,
            default=str,
        )


def set_logging_options() -> None:
    LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
    )

    logging.getLogger("py4j").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

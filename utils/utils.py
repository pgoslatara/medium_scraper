import json
import logging
import os
from functools import lru_cache
from glob import glob

import duckdb
from dbt.cli.main import dbtRunner


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
def get_json_content() -> list:
    contents = []
    for file_name in glob(f"{get_output_dir()}/*/*/*.json"):
        with open(file_name) as f:
            d = json.loads(f.read())
            for x in d:
                x["file_name"] = file_name
        contents += d

    logging.info(f"Read {len(contents)} blogs from json files")
    return contents


@lru_cache
def initialise_duckdb() -> duckdb.connect:
    return duckdb.connect(database=":memory:")


def run_dbt_command(command: str) -> None:
    dbt = dbtRunner()
    dbt.invoke(
        command.split(" ")[1:]
        + [
            "--profiles-dir",
            "./dbt",
            "--project-dir",
            "./dbt",
            "--target",
            get_environment(),
        ]
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

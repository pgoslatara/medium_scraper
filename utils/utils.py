import json
import logging
import os
import time
import uuid
from datetime import datetime
from functools import lru_cache
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union

import requests
from sh import dbt


class GitHubAPIRateLimitError(Exception):
    def __init__(self) -> None:
        print(self.__str__())

    def __str__(self) -> str:
        return f"GitHubAPIRateLimitError: API allocations resets at {self.get_api_reset_time()}."

    def get_api_reset_time(self) -> datetime:
        r = call_github_api(
            "GET",
            "rate_limit",
        )
        return datetime.fromtimestamp(r["rate"]["reset"])


def call_github_api(
    method: str,
    endpoint: str,
    params: Optional[Mapping[str, Union[int, str]]] = None,
) -> Any:
    if method.lower() == "get":
        r = requests.get(
            f"https://api.github.com/{endpoint}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {os.getenv('PAT_GITHUB')}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            params=params,
        )

    if (
        isinstance(r.json(), dict)
        and r.json().get("message")
        and r.json()["message"].startswith("API rate limit exceeded for user ID")
    ):
        try:
            raise GitHubAPIRateLimitError
        except GitHubAPIRateLimitError:
            logging.info("Retrying in 60 seconds...")
            time.sleep(60)
            call_github_api(
                method,
                endpoint,
                params,
            )

    return r.json()


@lru_cache
def get_environment() -> str:
    if os.getenv("GITHUB_TOKEN"):
        env = "prod"
    else:
        env = "dev"

    logging.info(f"Running on environment: {env}")

    return env


@lru_cache
def get_extracted_at() -> datetime:
    return datetime.utcnow()


@lru_cache
def get_extracted_at_epoch() -> int:
    return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())


@lru_cache
def get_extraction_id() -> str:
    return str(uuid.uuid4())


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

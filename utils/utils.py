import json
import os
import time
import uuid
from datetime import datetime
from functools import lru_cache, partial
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union

import cloudscraper  # type: ignore[import-not-found]
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.events.base_types import EventMsg

from utils.logger import logger


class GitHubAPIRateLimitError(Exception):
    def __init__(self) -> None:
        logger.info(self.__str__())

    def __str__(self) -> str:
        return f"GitHubAPIRateLimitError: API allocations resets at {self.get_api_reset_time()}."

    def get_api_reset_time(self) -> datetime:
        r = call_github_api(
            "GET",
            "rate_limit",
        )
        return datetime.fromtimestamp(r["rate"]["reset"])


class GitHubGraphqlRateLimitError(Exception):
    def __init__(self) -> None:
        logger.info(self.__str__())

    def __str__(self) -> str:
        return (
            f"GitHubGraphqlRateLimitError: API allocations resets at {self.get_api_reset_time()}."
        )

    def get_api_reset_time(self) -> datetime:
        r = call_github_api(
            "GET",
            "rate_limit",
        )
        return datetime.fromtimestamp(r["resources"]["graphql"]["reset"])


def call_github_api(
    method: str,
    endpoint: Optional[str] = None,
    json: Optional[Mapping[str, Union[int, str]]] = None,
    params: Optional[Mapping[str, Union[int, str]]] = None,
) -> Any:
    if method.lower() == "get":
        r = create_requests_session().get(
            f"https://api.github.com/{endpoint}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {os.getenv('PAT_GITHUB')}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            params=params,
        )
    elif method.lower() == "graphql":
        r = create_requests_session().post(
            url="https://api.github.com/graphql",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {os.getenv('PAT_GITHUB')}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json=json,
        )
    if isinstance(r.json(), dict) and (
        (
            r.json().get("message")
            and r.json()["message"].startswith("API rate limit exceeded for user ID")
        )
        or (
            method.lower() == "graphql"
            and "errors" in r.json()
            and (
                r.json().get("errors")[0].get("type") in ["RATE_LIMITED"]
                or r.json().get("errors")[0].get("message")
                in ["A query attribute must be specified and must be a string."]
            )
        )
    ):
        try:
            if method.lower() == "graphql":
                raise GitHubGraphqlRateLimitError
            else:
                raise GitHubAPIRateLimitError
        except (GitHubAPIRateLimitError, GitHubGraphqlRateLimitError):
            logger.info("Retrying in 60 seconds...")
            time.sleep(60)
            return call_github_api(
                method,
                endpoint,
                params,
            )

    logger.debug(f"Response: {r.status_code} {r.reason}")
    return r.json()


@lru_cache
def create_requests_session() -> cloudscraper.Session:
    logger.info("Creating re-usable requests session...")
    return cloudscraper.create_scraper()


@lru_cache
def get_environment() -> str:
    if os.getenv("GITHUB_TOKEN"):
        env = "prod"
    else:
        env = "dev"

    logger.info(f"Running on environment: {env}")

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

    logger.info(f"Read {len(contents)} blogs from json files")
    return contents


def dbt_invoke(dbt_cli_args: List[str], suppress_log: bool = False) -> dbtRunnerResult:
    """
    Takes a list of dbt command line arguments and calls `invoke` with
    a custom callback that makes logs available.
    """

    def capture_dbt_log(event: EventMsg, log_level: int, suppress_log: bool) -> None:
        from utils.logger import logger  # needs to be inside callback function

        # Small clean up of event data from dbt
        cleaned_info = {
            "level": event.info.level,
            "msg": event.info.msg,
            "name": event.info.name,
            "time": datetime.fromtimestamp(
                event.info.ts.seconds + (event.info.ts.nanos * (10**-9))
            ),
        }

        log_level_map = {
            "debug": 10,
            "info": 20,
            "warn": 30,
            "error": 40,
        }

        # This callback runs for every log message, we want to only log events
        # that have a log level >= the log level the package is running with.
        if suppress_log is False and log_level <= log_level_map[cleaned_info["level"]]:
            # i.e log message
            if cleaned_info["level"] == "debug":
                logger.debug(cleaned_info["msg"])
            elif cleaned_info["level"] == "info":
                logger.info(cleaned_info["msg"])
            elif cleaned_info["level"] == "warn":
                logger.warning(cleaned_info["msg"])
            elif cleaned_info["level"] == "error":
                logger.error(cleaned_info["msg"])

    standard_cli_args = [
        "--project-dir",
        os.getenv("DBT_PROJECT_DIR"),
        "--profiles-dir",
        os.getenv("DBT_PROFILES_DIR"),
        "--log-level",
        "none",  # logger will be handled by callback function
    ]
    args_to_pass = dbt_cli_args + standard_cli_args

    logger.info(f"Calling `dbtRunner.invoke` with args: {args_to_pass}")

    res = dbtRunner(
        callbacks=[
            partial(
                capture_dbt_log,
                log_level=logger.getEffectiveLevel(),
                suppress_log=suppress_log,
            )
        ]
    ).invoke(args_to_pass)

    if res.exception:
        raise RuntimeError(res.exception)
    return res


def save_to_landing_zone(data: List[Dict[str, object]], file_name: str) -> None:
    file_name = f"{get_output_dir()}/{file_name}"
    logger.info(f"Saving {len(data)} entries to {file_name}...")

    Path(file_name[: file_name.rfind("/")]).mkdir(parents=True, exist_ok=True)
    with open(file_name, "w", encoding="utf-8") as f_write:
        json.dump(
            data,
            f_write,
            ensure_ascii=False,
            indent=4,
            default=str,
        )

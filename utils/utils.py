import json
import os
import time
import uuid
from datetime import datetime
from functools import lru_cache, partial
from glob import glob
from pathlib import Path
from typing import Any, Collection, Dict, List, Mapping, Optional, Union

import cloudscraper  # type: ignore[import-not-found]
import pytz
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt_common.events.base_types import EventMsg  # type: ignore[import-not-found]
from graphql_query import (  # type: ignore[import-not-found]
    Argument,
    Field,
    Operation,
    Query,
)
from retry import retry

from utils.logger import logger

# GitHub throttles in two distinct ways. A primary rate limit publishes a reset time
# that can be polled, whereas a secondary (abuse detection) limit publishes nothing and
# only clears by backing off.
RATE_LIMIT_POLL_SECONDS = 60
RATE_LIMIT_MAX_WAIT_SECONDS = 60 * 90  # Primary allocations reset hourly.
SECONDARY_RATE_LIMIT_BACKOFF_SECONDS = 60
SECONDARY_RATE_LIMIT_MAX_RETRIES = 5
# Backstop against a response that always looks retryable, e.g. a malformed query
# returning no data key while the allocation is healthy.
MAX_RETRIES = 20


class GitHubAPIRateLimitError(Exception):
    """The REST API primary rate limit did not reset within the allowed wait."""


class GitHubGraphqlRateLimitError(Exception):
    """The GraphQL API primary rate limit did not reset within the allowed wait."""


class GitHubSecondaryRateLimitError(Exception):
    """GitHub applied a secondary rate limit that did not clear after backing off."""


def get_rest_api_reset_time() -> Optional[datetime]:
    """Return when the REST allocation resets, or None if it could not be read.

    The rate_limit endpoint does not itself consume allocation, but it can still be
    refused under a secondary rate limit, hence the optional return.
    """
    r = call_github_api("GET", "rate_limit", raise_on_rate_limit=False)
    if not isinstance(r, dict) or "rate" not in r:
        logger.warning(f"Could not read REST rate limit state: {r=}")
        return None

    return datetime.fromtimestamp(r["rate"]["reset"])


def get_graphql_api_reset_info() -> Optional[dict[str, Union[int, datetime, str]]]:
    """Return the GraphQL allocation state, or None if it could not be read."""
    ratelimit_query = Query(
        name="rateLimit",
        fields=[
            Field(name="limit"),
            Field(name="remaining"),
            Field(name="used"),
            Field(name="resetAt"),
        ],
    )

    # raise_on_rate_limit=False is essential: this probe runs *because* a call was
    # throttled, so re-entering rate limit handling here would recurse indefinitely.
    r = call_github_api(
        method="graphql",
        json={"query": Operation(type="query", queries=[ratelimit_query]).render()},
        raise_on_rate_limit=False,
    )
    rate_limit = r.get("data", {}).get("rateLimit") if isinstance(r, dict) else None
    if rate_limit is None:
        logger.warning(f"Could not read GraphQL rate limit state: {r=}")
        return None

    return {
        "remaining": rate_limit["remaining"],
        "used": rate_limit["used"],
        "resetAt": pytz.utc.localize(
            datetime.strptime(rate_limit["resetAt"], "%Y-%m-%dT%H:%M:%SZ")
        ),
    }


def is_secondary_rate_limit(payload: Any) -> bool:
    """Detect a secondary rate limit response from either the REST or GraphQL API.

    The two APIs link to different documentation anchors, so match on the shared
    substring as well as on the message text.
    """
    if not isinstance(payload, dict):
        return False

    documentation_url = payload.get("documentation_url") or ""
    message = payload.get("message") or ""

    return "secondary-rate-limits" in documentation_url or "secondary rate limit" in message


def is_retryable_response(method: str, payload: Any) -> bool:
    """Detect responses that clear by waiting: rate limits and transient API errors."""
    if not isinstance(payload, dict):
        return False

    if is_secondary_rate_limit(payload):
        return True

    message = payload.get("message") or ""
    if message.startswith("API rate limit exceeded for user ID"):
        return True

    if method.lower() != "graphql":
        return False

    errors = payload.get("errors")
    if errors:
        first_error = errors[0]
        if first_error.get("type") == "RATE_LIMITED" or first_error.get("message") in [
            "A query attribute must be specified and must be a string."
        ]:
            return True

    # A GraphQL response with no data key is an error or a server-side timeout, both
    # of which are worth retrying.
    return "data" not in payload.keys()


def request_github_api(
    method: str,
    endpoint: Optional[str] = None,
    json: Optional[Mapping[str, Union[int, str]]] = None,
    params: Optional[Mapping[str, Union[int, str]]] = None,
) -> Any:
    """Issue a single GitHub API request with no rate limit handling."""
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {os.getenv('PAT_GITHUB')}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    if method.lower() == "get":
        return create_requests_session().get(
            f"https://api.github.com/{endpoint}",
            headers=headers,
            params=params,
        )
    elif method.lower() == "graphql":
        return create_requests_session().post(
            url="https://api.github.com/graphql",
            headers=headers,
            json=json,
        )

    raise ValueError(f"Unsupported GitHub API method: {method}")


def wait_for_rest_rate_limit_reset() -> None:
    """Poll until the REST allocation is available again."""
    deadline = time.monotonic() + RATE_LIMIT_MAX_WAIT_SECONDS
    while time.monotonic() < deadline:
        # Sleep first so that callers always back off before their next request.
        time.sleep(RATE_LIMIT_POLL_SECONDS)
        reset_time = get_rest_api_reset_time()
        if reset_time is None:
            logger.info("REST rate limit query was refused, continuing to wait...")
        elif reset_time <= datetime.now():
            return
        else:
            logger.info(f"Waiting until {reset_time} for REST allocation to reset...")

    raise GitHubAPIRateLimitError(
        f"REST allocation still exhausted after {RATE_LIMIT_MAX_WAIT_SECONDS} seconds."
    )


def wait_for_graphql_rate_limit_reset() -> None:
    """Poll until the GraphQL allocation is available again."""
    deadline = time.monotonic() + RATE_LIMIT_MAX_WAIT_SECONDS
    while time.monotonic() < deadline:
        # Sleep first so that callers always back off before their next request.
        time.sleep(RATE_LIMIT_POLL_SECONDS)
        reset_info = get_graphql_api_reset_info()
        if reset_info is None:
            logger.info("GraphQL rate limit query was refused, continuing to wait...")
        elif int(reset_info["remaining"]) > 0:  # type: ignore[arg-type]
            return
        else:
            logger.info(f"Waiting until {reset_info['resetAt']} UTC...")

    raise GitHubGraphqlRateLimitError(
        f"GraphQL allocation still exhausted after {RATE_LIMIT_MAX_WAIT_SECONDS} seconds."
    )


def call_github_api(
    method: str,
    endpoint: Optional[str] = None,
    json: Optional[Mapping[str, Union[int, str]]] = None,
    params: Optional[Mapping[str, Union[int, str]]] = None,
    raise_on_rate_limit: bool = True,
) -> Any:
    """Call the GitHub REST or GraphQL API, waiting out any rate limits.

    Set raise_on_rate_limit to False to receive the raw payload instead of triggering
    rate limit handling; the rate limit probes rely on this to avoid recursing.
    """
    secondary_rate_limit_retries = 0

    for attempt in range(MAX_RETRIES + 1):
        r = request_github_api(method=method, endpoint=endpoint, json=json, params=params)
        payload = r.json()

        if not raise_on_rate_limit or not is_retryable_response(method, payload):
            logger.debug(f"Response: {r.status_code} {r.reason}")
            return payload

        logger.warning(f"Error detected on attempt {attempt + 1}, returned data: {payload=}")

        if is_secondary_rate_limit(payload):
            secondary_rate_limit_retries += 1
            if secondary_rate_limit_retries > SECONDARY_RATE_LIMIT_MAX_RETRIES:
                raise GitHubSecondaryRateLimitError(
                    f"Still secondary rate limited after {SECONDARY_RATE_LIMIT_MAX_RETRIES} retries."
                )

            # Secondary limits publish no reset time, so back off exponentially.
            backoff_seconds = SECONDARY_RATE_LIMIT_BACKOFF_SECONDS * 2 ** (
                secondary_rate_limit_retries - 1
            )
            logger.info(f"Secondary rate limit hit, retrying in {backoff_seconds} seconds...")
            time.sleep(backoff_seconds)
        elif method.lower() == "graphql":
            wait_for_graphql_rate_limit_reset()
        else:
            wait_for_rest_rate_limit_reset()

    raise GitHubAPIRateLimitError(
        f"GitHub API still returning retryable errors after {MAX_RETRIES} retries."
    )


@lru_cache
def create_requests_session() -> cloudscraper.Session:
    logger.info("Creating re-usable requests session...")
    return cloudscraper.create_scraper()


@lru_cache
def get_environment() -> str:
    if os.getenv("CICD_RUN") == "True":
        env = "dev"
    else:
        env = "prod"

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

    if not res.success:
        raise RuntimeError(f"dbt_invoke failed with args: {dbt_cli_args}")
    return res


@retry(delay=5, tries=5)
def medium_post_graphql_request(
    headers: dict[str, str], json: list[dict[str, Collection[str]]]
) -> dict[str, Any]:
    return (  # type: ignore[no-any-return]
        create_requests_session()
        .post("https://medium.com/_/graphql", headers=headers, json=json)
        .json()
    )


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

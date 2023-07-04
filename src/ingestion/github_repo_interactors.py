import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List

from utils.utils import (
    call_github_api,
    get_extracted_at,
    get_extracted_at_epoch,
    get_extraction_id,
    save_to_landing_zone,
    set_logging_options,
)


def get_github_repos_per_org(org: str) -> List[Dict[str, object]]:
    org_repos = []
    page = 1
    _repos: List[Dict[str, object]] = []
    while page == 1 or _repos != []:
        _repos = call_github_api(
            "GET", f"orgs/{org}/repos", params={"per_page": 100, "page": page}
        )
        org_repos += _repos
        logging.info(f"Retrieved {len(org_repos)} repos from {org}...")
        page += 1

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for repo in org_repos:
        repo = repo.update(metadata)  # type: ignore[assignment]
    save_to_landing_zone(
        data=org_repos,
        file_name=f"domain=github_repos/schema_version=1/org={org}/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )
    return org_repos


def get_github_issues(repos: List[object]) -> List[Dict[str, object]]:
    _since = (
        (datetime.utcnow() - timedelta(days=3))
        if os.getenv("CICD_RUN")
        else datetime(1900, 1, 1)
    )
    logging.info(f"{_since=}")

    issues: List[Dict[str, object]] = []
    for repo in repos:
        page = 1
        while True:
            issues_retrieved = call_github_api(
                "GET",
                f"repos/{repo}/issues",
                params={
                    "state": "all",
                    "sort": "created",
                    "direction": "desc",
                    "per_page": 100,
                    "page": page,
                    "since": str(_since),
                },
            )
            logging.info(f"Retrieved {len(issues_retrieved)} issues from {repo}...")
            issues += issues_retrieved
            page += 1
            if len(issues_retrieved) == 0:
                break

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for issue in issues:
        issue = issue.update(metadata)  # type: ignore[assignment]
    save_to_landing_zone(
        data=issues,
        file_name=f"domain=github_issues/schema_version=1/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )
    return issues


def get_github_pull_requests(repos: List[object]) -> List[Dict[str, object]]:
    _state = "open" if os.getenv("CICD_RUN") else "all"
    logging.info(f"{_state=}")

    pull_requests: List[Dict[str, object]] = []
    for repo in repos:
        page = 1
        while True:
            pull_requests_retrieved = call_github_api(
                "GET",
                f"repos/{repo}/pulls",
                params={
                    "state": _state,
                    "sort": "created",
                    "direction": "desc",
                    "per_page": 100,
                    "page": page,
                },
            )
            logging.info(f"Retrieved {len(pull_requests_retrieved)} PRs from {repo}...")
            pull_requests += pull_requests_retrieved
            page += 1
            if len(pull_requests_retrieved) == 0 or (
                os.getenv("CICD_RUN") and page > 2
            ):
                break

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for pr in pull_requests:
        pr = pr.update(metadata)  # type: ignore[assignment]
    save_to_landing_zone(
        data=pull_requests,
        file_name=f"domain=github_pull_requests/schema_version=1/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )
    return pull_requests


def get_github_repo_interactor_info(usernames: List[object]) -> List[Dict[str, object]]:
    if os.getenv("CICD_RUN"):
        usernames = usernames[:50]

    user_info = [call_github_api("GET", f"users/{username}") for username in usernames]
    logging.info(f"Retrieved info on {len(usernames)} repo interactors.")

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for user in user_info:
        user = user.update(metadata)
    save_to_landing_zone(
        data=user_info,
        file_name=f"domain=github_users/schema_version=1/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )
    return user_info


def main() -> None:
    set_logging_options()

    github_orgs = ["dbt-labs"]
    for org in github_orgs:
        repos = get_github_repos_per_org(org)
        repo_names = [x["full_name"] for x in repos if not x["fork"]]
        if os.getenv("CICD_RUN"):
            repo_names = repo_names[:50]

        logging.info(
            f"Retrieving issues and PRs for {len(repo_names)} non-forked repos."
        )
        issues = get_github_issues(repo_names)
        prs = get_github_pull_requests(repo_names)
        repo_interactors = {x["user"]["login"] for x in prs + issues}  # type: ignore[index]
        logging.info(f"Extracted {len(repo_interactors)} unique GitHub usernames.")
        get_github_repo_interactor_info(list(repo_interactors))


if __name__ == "__main__":
    main()

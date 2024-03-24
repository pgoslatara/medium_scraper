import os
from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool
from typing import Dict, List

from graphql_query import (  # type: ignore[import-not-found]
    Argument,
    Field,
    Operation,
    Query,
)

from utils.logger import logger
from utils.utils import (
    call_github_api,
    get_extracted_at,
    get_extracted_at_epoch,
    get_extraction_id,
    save_to_landing_zone,
)


def get_github_repos_per_org(org: str) -> List[Dict[str, object]]:
    org_repos = []
    page = 1
    _repos: List[Dict[str, object]] = []
    while page == 1 or _repos != []:
        _repos = call_github_api(
            "GET", f"orgs/{org}/repos", params={"per_page": 100, "page": page}
        )
        keys_to_keep = ["id", "name", "full_name", "html_url", "url", "fork"]
        org_repos += [{k: v for k, v in x.items() if k in keys_to_keep} for x in _repos]
        logger.info(f"Retrieved {len(org_repos)} repos from {org}...")
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


def get_github_discussions(repos: List[str]) -> None:
    overall_discussions: List[Dict[str, object]] = []
    for repo in repos:
        logger.debug(f"Fetching discussions from {repo=}...")

        repo_query = Query(
            name="repository",
            arguments=[
                Argument(name="owner", value=f'"{repo.split("/")[0]}"'),
                Argument(name="name", value=f'"{repo.split("/")[1]}"'),
            ],
            fields=[
                Field(
                    name="discussions",
                    arguments=[
                        Argument(name="first", value="100"),
                        Argument(name="after", value="null"),
                    ],
                    fields=[
                        "totalCount",
                        Field(
                            name="pageInfo",
                            fields=["startCursor", "endCursor", "hasNextPage", "hasPreviousPage"],
                        ),
                        Field(
                            name="edges",
                            fields=[
                                Field(
                                    name="node",
                                    fields=[
                                        Field(name="author", fields=["login"]),
                                        Field(name="category", fields=["name"]),
                                        "createdAt",
                                        "id",
                                        "number",
                                        "title",
                                        "url",
                                    ],
                                ),
                            ],
                        ),
                    ],
                )
            ],
        )

        discussions = []
        endCursor = "null"  # Initialising
        while endCursor == "null" or endCursor is not None:
            r = call_github_api(
                method="graphql",
                json={"query": Operation(type="query", queries=[repo_query]).render()},
            )
            endCursor = r["data"]["repository"]["discussions"]["pageInfo"]["endCursor"]
            repo_query.fields[0].arguments[1].value = f'"{endCursor}"'
            for disc in r["data"]["repository"]["discussions"]["edges"]:
                discussions.append(disc["node"])

        logger.info(f"Retrieved {len(discussions)} from {repo}...")
        for d in discussions:
            overall_discussions.append(d)

    logger.info(f"Retrieved {len(overall_discussions)} in total...")

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for d in overall_discussions:
        d = d.update(metadata)  # type: ignore[assignment]
    save_to_landing_zone(
        data=overall_discussions,
        file_name=f"domain=github_discussions/schema_version=1/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )


def get_github_issues(repos: List[str]) -> List[Dict[str, object]]:
    def get_issues_in_repo(repo: str) -> List[Dict[str, object]]:
        issues: List[Dict[str, object]] = []
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
            logger.info(f"Retrieved {len(issues_retrieved)} issues from {repo}...")
            keys_to_keep = [
                "created_at",
                "html_url",
                "id",
                "number",
                "repository_url",
                "state",
                "title",
                "user",
            ]
            issues += [{k: v for k, v in x.items() if k in keys_to_keep} for x in issues_retrieved]
            page += 1
            if len(issues_retrieved) == 0:
                break

        return issues

    _since = (
        (datetime.utcnow() - timedelta(days=3))
        if os.getenv("CICD_RUN") == "True"
        else datetime(1900, 1, 1)
    )
    logger.info(f"{_since=}")

    pool = ThreadPool(1)
    issues = [
        x
        for y in pool.map(
            lambda repo: get_issues_in_repo(repo),
            repos,
        )
        for x in y
    ]

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


def get_github_pull_requests(repos: List[str]) -> List[Dict[str, object]]:
    def get_pull_requests_in_repo(repo: str) -> List[Dict[str, object]]:
        pull_requests: List[Dict[str, object]] = []
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
            logger.info(f"Retrieved {len(pull_requests_retrieved)} PRs from {repo}...")
            keys_to_keep = [
                "created_at",
                "html_url",
                "id",
                "number",
                "repository_url",
                "state",
                "title",
                "user",
            ]
            pull_requests += [
                {k: v for k, v in x.items() if k in keys_to_keep} for x in pull_requests_retrieved
            ]
            page += 1
            if len(pull_requests_retrieved) == 0 or (os.getenv("CICD_RUN") == "True" and page > 2):
                break

        return pull_requests

    _state = "open" if os.getenv("CICD_RUN") == "True" else "all"
    logger.info(f"{_state=}")

    pool = ThreadPool(1)
    pull_requests = [
        x
        for y in pool.map(
            lambda repo: get_pull_requests_in_repo(repo),
            repos,
        )
        for x in y
    ]

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
    if os.getenv("CICD_RUN") == "True":
        usernames = usernames[:10]

    pool = ThreadPool(1)
    user_info = pool.map(
        lambda username: call_github_api("GET", f"users/{username}"),
        usernames,
    )
    logger.info(f"Retrieved info on {len(usernames)} repo interactors.")

    keys_to_keep = [
        "bio",
        "blog",
        "company",
        "email",
        "id",
        "html_url",
        "location",
        "login",
        "name",
        "twitter_username",
    ]
    user_info = [{k: v for k, v in x.items() if k in keys_to_keep} for x in user_info]

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
    github_orgs = ["dbt-labs"]
    for org in github_orgs:
        repos = get_github_repos_per_org(org)
        repo_names = [str(x["full_name"]) for x in repos if not x["fork"]]
        if os.getenv("CICD_RUN") == "True":
            repo_names = repo_names[:10]

        logger.info(f"Retrieving issues and PRs for {len(repo_names)} non-forked repos.")
        issues = get_github_issues(repo_names)
        prs = get_github_pull_requests(repo_names)

        repo_interactors = {x["user"]["login"] for x in prs + issues}  # type: ignore[index]
        logger.info(f"Extracted {len(repo_interactors)} unique GitHub usernames.")
        get_github_repo_interactor_info(list(repo_interactors))

        logger.info(f"Retrieving discussions for {len(repo_names)} non-forked repos.")
        get_github_discussions(repo_names)


if __name__ == "__main__":
    main()

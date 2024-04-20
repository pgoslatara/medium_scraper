import os
from multiprocessing.pool import ThreadPool
from typing import Any, Dict, List

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
    logger.info(f"Fetching repos from {org=}...")

    org_query = Query(
        name="organization",
        arguments=[
            Argument(name="login", value=f'"{org}"'),
        ],
        fields=[
            Field(
                name="repositories",
                arguments=[
                    Argument(name="first", value="100"),
                    Argument(name="after", value="null"),
                ],
                fields=[
                    "totalCount",
                    # Field(name="organization"),
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
                                    "createdAt",
                                    "databaseId",
                                    "isFork",
                                    "name",
                                    "nameWithOwner",
                                    "url",
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    r = call_github_api(
        method="graphql",
        json={"query": Operation(type="query", queries=[org_query]).render()},
    )
    repos = []
    endCursor = "null"  # Initialising
    while endCursor == "null" or endCursor is not None:
        r = call_github_api(
            method="graphql",
            json={"query": Operation(type="query", queries=[org_query]).render()},
        )
        endCursor = r["data"]["organization"]["repositories"]["pageInfo"]["endCursor"]
        org_query.fields[0].arguments[1].value = f'"{endCursor}"'
        for i in r["data"]["organization"]["repositories"]["edges"]:
            repos.append(i["node"])

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for repo in repos:
        repo = repo.update(metadata)
    save_to_landing_zone(
        data=repos,
        file_name=f"domain=github_repos/schema_version=2/org={org}/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )
    return repos


def get_github_discussions(repos: List[str]) -> None:
    def get_discussions(repo: str) -> Any:
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

        logger.info(f"Retrieved {len(discussions)} discussions from {repo}...")
        return discussions

    pool = ThreadPool(8)
    overall_discussions = pool.map(
        lambda repo: get_discussions(repo),
        repos,
    )
    overall_discussions = [discussion for r in overall_discussions for discussion in r]

    logger.info(f"Retrieved {len(overall_discussions)} discussions in total...")

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for d in overall_discussions:
        d = d.update(metadata)
    save_to_landing_zone(
        data=overall_discussions,
        file_name=f"domain=github_discussions/schema_version=1/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )


def get_github_issues(repos: List[str]) -> List[Dict[str, object]]:
    def get_issue_info(repo: str) -> Any:
        logger.info(f"Fetching issues from {repo=}...")

        issue_query = Query(
            name="repository",
            arguments=[
                Argument(name="owner", value=f'"{repo.split("/")[0]}"'),
                Argument(name="name", value=f'"{repo.split("/")[1]}"'),
            ],
            fields=[
                Field(
                    name="issues",
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
                                        Field(
                                            name="author", fields=["login"]
                                        ),  # formerly user.login
                                        Field(name="createdAt"),
                                        Field(name="databaseId"),
                                        Field(name="number"),
                                        Field(name="repository", fields=[Field(name="url")]),
                                        Field(name="state"),
                                        Field(name="title"),
                                        Field(name="url"),  # formerly html_url
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )

        r = call_github_api(
            method="graphql",
            json={"query": Operation(type="query", queries=[issue_query]).render()},
        )
        issues = []
        endCursor = "null"  # Initialising
        while endCursor == "null" or endCursor is not None:
            r = call_github_api(
                method="graphql",
                json={"query": Operation(type="query", queries=[issue_query]).render()},
            )
            endCursor = r["data"]["repository"]["issues"]["pageInfo"]["endCursor"]
            issue_query.fields[0].arguments[1].value = f'"{endCursor}"'
            for i in r["data"]["repository"]["issues"]["edges"]:
                issues.append(i["node"])

        logger.info(f"Retrieved {len(issues)} issues from {repo}...")
        return issues

    pool = ThreadPool(8)
    overall_issues = pool.map(
        lambda repo: get_issue_info(repo),
        repos,
    )
    overall_issues = [issue for r in overall_issues for issue in r]

    logger.info(f"Retrieved {len(overall_issues)} issues in total...")

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for issue in overall_issues:
        issue = issue.update(metadata)
    save_to_landing_zone(
        data=overall_issues,
        file_name=f"domain=github_issues/schema_version=2/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )
    return overall_issues


def get_github_pull_requests(repos: List[str]) -> List[Dict[str, object]]:
    def get_pr_info(repo: str) -> Any:
        logger.info(f"Fetching PRs from {repo=}...")

        pr_query = Query(
            name="repository",
            arguments=[
                Argument(name="owner", value=f'"{repo.split("/")[0]}"'),
                Argument(name="name", value=f'"{repo.split("/")[1]}"'),
            ],
            fields=[
                Field(
                    name="pullRequests",
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
                                        Field(
                                            name="author", fields=["login"]
                                        ),  # formerly user.login
                                        Field(name="createdAt"),
                                        Field(name="databaseId"),
                                        Field(name="number"),
                                        Field(
                                            name="repository", fields=[Field(name="url")]
                                        ),  # formerly html_url
                                        Field(name="state"),
                                        Field(name="title"),
                                        Field(name="url"),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )

        r = call_github_api(
            method="graphql",
            json={"query": Operation(type="query", queries=[pr_query]).render()},
        )
        prs = []
        endCursor = "null"  # Initialising
        while endCursor == "null" or endCursor is not None:
            r = call_github_api(
                method="graphql",
                json={"query": Operation(type="query", queries=[pr_query]).render()},
            )
            endCursor = r["data"]["repository"]["pullRequests"]["pageInfo"]["endCursor"]
            pr_query.fields[0].arguments[1].value = f'"{endCursor}"'
            for i in r["data"]["repository"]["pullRequests"]["edges"]:
                prs.append(i["node"])

        logger.info(f"Retrieved {len(prs)} PRs from {repo}...")
        return prs

    pool = ThreadPool(8)
    overall_prs = pool.map(
        lambda repo: get_pr_info(repo),
        repos,
    )
    overall_prs = [pr for r in overall_prs for pr in r]

    logger.info(f"Retrieved {len(overall_prs)} PRs in total...")

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for pr in overall_prs:
        pr = pr.update(metadata)
    save_to_landing_zone(
        data=overall_prs,
        file_name=f"domain=github_pull_requests/schema_version=2/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )
    return overall_prs


def get_github_repo_interactor_info(usernames: List[object]) -> List[Dict[str, object]]:
    def get_username_info(username: object) -> Any:
        logger.info(f"Fetching user: {username=}...")
        user_query = Query(
            name="user",
            arguments=[
                Argument(name="login", value=f'"{username}"'),
            ],
            fields=[
                Field(name="bio"),
                Field(name="websiteUrl"),  # formerly blog
                Field(name="company"),
                Field(name="email"),
                Field(name="url"),  # formerly html_url
                Field(name="databaseId"),  # formerly id
                Field(name="location"),
                Field(name="login"),
                Field(name="name"),
                Field(name="twitterUsername"),
            ],
        )

        return call_github_api(
            method="graphql",
            json={"query": Operation(type="query", queries=[user_query]).render()},
        )["data"]["user"]

    if os.getenv("CICD_RUN") == "True":
        usernames = usernames[:10]

    pool = ThreadPool(8)
    user_info = pool.map(
        lambda username: get_username_info(username),
        usernames,
    )
    logger.info(f"Retrieved info on {len(usernames)} repo interactors.")

    metadata = {
        "extraction_id": get_extraction_id(),
        "extracted_at": get_extracted_at(),
        "extracted_at_epoch": get_extracted_at_epoch(),
    }
    for user in user_info:
        user = user.update(metadata)
    save_to_landing_zone(
        data=user_info,
        file_name=f"domain=github_users/schema_version=2/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
    )
    return user_info


def main() -> None:
    github_orgs = ["dbt-labs"]
    for org in github_orgs:
        repos = get_github_repos_per_org(org)
        repo_names = [str(x["nameWithOwner"]) for x in repos if not x["isFork"]]
        if os.getenv("CICD_RUN") == "True":
            repo_names = repo_names[:10]

        logger.info(f"Retrieving issues and PRs for {len(repo_names)} non-forked repos.")
        issues = get_github_issues(repo_names)
        prs = get_github_pull_requests(repo_names)

        # Accounting for accounts that have been deleted
        repo_interactors = [
            y
            for y in {
                (x.get("author").get("login") if x.get("author") is not None else x.get("author"))  # type: ignore[attr-defined]
                for x in prs + issues
            }
            if y is not None
        ]
        logger.info(f"Extracted {len(repo_interactors)} unique GitHub usernames.")
        get_github_repo_interactor_info(list(repo_interactors))

        logger.info(f"Retrieving discussions for {len(repo_names)} non-forked repos.")
        get_github_discussions(repo_names)


if __name__ == "__main__":
    main()

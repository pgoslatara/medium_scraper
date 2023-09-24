import os

from src.ingestion.github_actions import GitHubActionsExtractor
from src.ingestion.github_repo_interactors import main as github_repo_interactors_run
from src.ingestion.medium_web_scraper import MediumWebScraper
from utils.utils import set_logging_options


def main() -> None:
    set_logging_options()

    default_lookback = 7
    try:
        lookback_days = int(os.getenv("INGESTION_LOOKBACK_WINDOW", default_lookback))
    except:
        lookback_days = default_lookback

    default_medium_blog_limit = 100
    try:
        medium_blog_limit = int(
            os.getenv("INGESTION_MEDIUM_BLOG_LIMIT", default_medium_blog_limit)
        )
    except:
        medium_blog_limit = default_medium_blog_limit

    tags = [
        "analytics-engineering",
        "apache-airflow",
        "databricks",
        "dbt",
    ]

    GitHubActionsExtractor(lookback_days=lookback_days).run()
    github_repo_interactors_run()
    MediumWebScraper(
        medium_blog_limit=medium_blog_limit,
        tags=tags,
    ).run()


if __name__ == "__main__":
    main()

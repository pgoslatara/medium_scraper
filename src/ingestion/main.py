import os

from src.ingestion.github_actions import GitHubActionsExtractor
from src.ingestion.github_repo_interactors import main as github_repo_interactors_run
from src.ingestion.medium_web_scraper import MediumWebScraper


def main() -> None:
    default_lookback = 7
    try:
        lookback_days = int(os.getenv("INGESTION_LOOKBACK_WINDOW", default_lookback))
    except:
        lookback_days = default_lookback

    tags = [
        "analytics-engineering",
        "apache-airflow",
        "databricks",
        "dbt",
    ]

    GitHubActionsExtractor(lookback_days=lookback_days).run()
    github_repo_interactors_run()
    MediumWebScraper(
        tags=tags,
    ).run()


if __name__ == "__main__":
    main()

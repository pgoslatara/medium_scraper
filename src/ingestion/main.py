import os

from utils.utils import set_logging_options

from .github_actions import GitHubActionsExtractor
from .medium_web_scraper import MediumWebScraper


def main() -> None:
    set_logging_options()

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
    MediumWebScraper(
        lookback_days=lookback_days,
        tags=tags,
    ).run()


if __name__ == "__main__":
    main()

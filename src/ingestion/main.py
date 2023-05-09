import os

from utils.utils import set_logging_options

from .medium_web_scraper import MediumWebScraper


def main():
    set_logging_options()

    # MEDIUM_BLOG_LOOKBACK_WINDOW is "" when action is triggered from main branch
    default_lookback = 7
    try:
        lookback_days = int(os.getenv("MEDIUM_BLOG_LOOKBACK_WINDOW", default_lookback))
    except:
        lookback_days = default_lookback

    tags = [
        "analytics-engineering",
        "apache-airflow",
        "databricks",
        "dbt",
    ]

    MediumWebScraper(
        lookback_days=lookback_days,
        tags=tags,
    ).run()


if __name__ == "__main__":
    main()

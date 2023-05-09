import os

from src.bi_assembler import BiAssembler
from src.email_sender import EmailSender
from src.medium_web_scraper import MediumWebScraper
from utils import utils


def main():
    utils.set_logging_options()

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

    extraction_id = MediumWebScraper(
        lookback_days=lookback_days,
        tags=tags,
    ).run()
    BiAssembler().run()
    EmailSender(lookback_days=lookback_days).run()


if __name__ == "__main__":
    main()

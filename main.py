from src.email_sender import EmailSender
from src.medium_web_scraper import MediumWebScraper
from utils import utils

if __name__ == "__main__":
    lookback_days = 7
    tags = [
        "analytics-engineering",
        "apache-airflow",
        "databricks",
        "dbt",
    ]

    utils.set_logging_options()
    extraction_id = MediumWebScraper(
        lookback_days=lookback_days,
        tags=tags,
    ).run()
    EmailSender(lookback_days=7).run()

from datetime import datetime, timedelta
import logging
from scraper.medium_web_scraper import MediumWebScraper
from utils import utils

if __name__ == "__main__":
    utils.set_logging_options()

    for tag in ["analytics-engineering", "apache-airflow", "databricks", "dbt"]:
        MediumWebScraper(lookback_days=7, tag=tag).run()

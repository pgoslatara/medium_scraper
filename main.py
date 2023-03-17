import os
from src.bi_assembler import BiAssembler
from src.email_sender import EmailSender
from src.medium_web_scraper import MediumWebScraper
from utils import utils

if __name__ == "__main__":
    utils.set_logging_options()

    # lookback_days = int(os.getenv('MEDIUM_BLOG_LOOKBACK_WINDOW', 7))
    # tags = [
    #     "analytics-engineering",
    #     "apache-airflow",
    #     "databricks",
    #     "dbt",
    # ]

    # extraction_id = MediumWebScraper(
    #     lookback_days=lookback_days,
    #     tags=tags,
    # ).run()
    BiAssembler().run()
    # EmailSender(lookback_days=lookback_days).run()

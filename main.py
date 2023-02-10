import duckdb
from scraper.medium_web_scraper import MediumWebScraper
from utils import utils

if __name__ == "__main__":
    utils.set_logging_options()
    MediumWebScraper(lookback_days=7, tags=[
        # "analytics-engineering",
        "apache-airflow",
        # "databricks",
        "dbt"
    ]).run()

    con = duckdb.connect(database=":memory:")
    # df = con.execute("SELECT * FROM read_json_objects('./local_output/*.json')")
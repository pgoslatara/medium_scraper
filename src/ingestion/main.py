from src.ingestion.github_repo_interactors import main as github_repo_interactors_run
from src.ingestion.medium_web_scraper import MediumWebScraper
from utils.utils import get_ingestion_lookback_days


def main() -> None:
    tags = [
        "analytics-engineering",
        "databricks",
        "dbt",
    ]

    github_repo_interactors_run()
    MediumWebScraper(
        lookback_days=get_ingestion_lookback_days(),
        tags=tags,
    ).run()


if __name__ == "__main__":
    main()

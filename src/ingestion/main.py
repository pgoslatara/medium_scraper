from src.ingestion.github_repo_interactors import main as github_repo_interactors_run
from src.ingestion.medium_web_scraper import MediumWebScraper


def main() -> None:
    tags = [
        "analytics-engineering",
        "apache-airflow",
        "databricks",
        "dbt",
    ]

    github_repo_interactors_run()
    MediumWebScraper(
        tags=tags,
    ).run()


if __name__ == "__main__":
    main()

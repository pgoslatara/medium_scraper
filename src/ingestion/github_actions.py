import logging
import uuid
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List

from utils.utils import *


class GitHubActionsExtractor:
    def __init__(self, lookback_days: int) -> None:
        self.lookback_days = lookback_days

    @lru_cache
    def get_extracted_at(self) -> datetime:
        return datetime.utcnow()

    @lru_cache
    def get_extracted_at_epoch(self) -> int:
        return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())

    @lru_cache
    def get_extraction_id(self) -> str:
        return str(uuid.uuid4())

    def run(self) -> None:
        logging.info("Extracting data from GitHub...")

        workflow_runs = self.extract_github_workflow_runs()
        save_to_landing_zone(
            data=workflow_runs,
            file_name=f"domain=github_actions_workflow_runs/schema_version=1/extracted_at={self.get_extracted_at_epoch()}/extraction_id={self.get_extraction_id()}.json",
        )

        jobs_data = self.extract_github_jobs(workflow_runs)
        save_to_landing_zone(
            data=jobs_data,
            file_name=f"domain=github_actions_jobs/schema_version=1/extracted_at={self.get_extracted_at_epoch()}/extraction_id={self.get_extraction_id()}.json",
        )

    def extract_github_workflow_runs(self) -> Any:
        created_filter = f">{(datetime.today() - timedelta(days=self.lookback_days)).date().strftime('%Y-%m-%d')}"
        logging.info(
            f"Extracting workflow data from GitHub (filter '{created_filter}')..."
        )
        per_page = 10

        repos = call_github_api("GET", "users/pgoslatara/repos")

        for repo in repos:
            logging.info(f"Repo name: {repo['name']}")
            workflows = call_github_api(
                "GET", f"repos/pgoslatara/{repo['name']}/actions/workflows"
            )["workflows"]
            for workflow in workflows:
                logging.info(f"Workflow name: {workflow['name']}")
                r = call_github_api(
                    "GET",
                    f"repos/pgoslatara/{repo['name']}/actions/workflows/{workflow['id']}/runs",
                    params={"created": created_filter, "page": 1, "per_page": per_page},
                )
                workflow_runs = r["workflow_runs"]
                total_count = r["total_count"]

                for page_num in list(range(2, int(total_count / per_page) + 2)):
                    r = call_github_api(
                        "GET",
                        f"repos/pgoslatara/{repo['name']}/actions/workflows/{workflow['id']}/runs",
                        params={
                            "created": created_filter,
                            "page": page_num,
                            "per_page": per_page,
                        },
                    )
                    workflow_runs += r["workflow_runs"]
                logging.info(f"Retrieved {len(workflow_runs)} workflows.")

        metadata = {
            "extraction_id": self.get_extraction_id(),
            "extracted_at": self.get_extracted_at(),
            "extracted_at_epoch": self.get_extracted_at_epoch(),
        }
        for workflow in workflow_runs:
            workflow = workflow.update(metadata)

        logging.info(f"Extracted data from {len(workflow_runs)} workflows from GitHub.")

        return workflow_runs

    def extract_github_jobs(
        self, workflow_runs: List[Dict[str, object]]
    ) -> List[Dict[str, object]]:
        logging.info("Extracting jobs data from GitHub...")

        jobs_data = []
        for workflow_run in workflow_runs:
            r = call_github_api(
                "GET",
                f"repos/pgoslatara/medium_scraper/actions/runs/{workflow_run['id']}/jobs",
            )
            if "message" not in r.keys():  # i.e. logs are still available
                for job in r["jobs"]:
                    jobs_data.append(job)

        metadata = {
            "extraction_id": self.get_extraction_id(),
            "extracted_at": self.get_extracted_at(),
            "extracted_at_epoch": self.get_extracted_at_epoch(),
        }
        for job in jobs_data:
            job = job.update(metadata)

        logging.info(f"Extracted data from {len(jobs_data)} jobs from GitHub.")

        return jobs_data

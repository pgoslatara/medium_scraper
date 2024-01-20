import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from retry import retry

from utils.logger import logger
from utils.utils import *


class GitHubActionsExtractor:
    def __init__(self, lookback_days: int) -> None:
        self.lookback_days = lookback_days

    def run(self) -> None:
        logger.info("Extracting data from GitHub...")

        workflow_runs = self.extract_github_workflow_runs()
        save_to_landing_zone(
            data=workflow_runs,
            file_name=f"domain=github_actions_workflow_runs/schema_version=1/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
        )

        jobs_data = self.extract_github_jobs(workflow_runs)
        save_to_landing_zone(
            data=jobs_data,
            file_name=f"domain=github_actions_jobs/schema_version=1/extracted_at={get_extracted_at_epoch()}/extraction_id={get_extraction_id()}.json",
        )

    @retry(tries=3, delay=5)
    def extract_github_workflow_runs(self) -> Any:
        created_filter = f">{(datetime.today() - timedelta(days=self.lookback_days)).date().strftime('%Y-%m-%d')}"
        logger.info(f"Extracting workflow run data from GitHub (filter '{created_filter}')...")
        per_page = 100

        repos = call_github_api(
            "GET", "user/repos", params={"per_page": per_page, "type": "owner"}
        )
        if os.getenv("CICD_RUN") == "True":
            repos = repos[0:5]

        workflow_runs = []
        for repo in repos:
            logger.info(f"Repo name: {repo['name']}")
            workflows = call_github_api(
                "GET", f"repos/pgoslatara/{repo['name']}/actions/workflows"
            )["workflows"]
            for workflow in workflows:
                logger.info(f"Workflow name: {workflow['name']}")
                r = call_github_api(
                    "GET",
                    f"repos/pgoslatara/{repo['name']}/actions/workflows/{workflow['id']}/runs",
                    params={"created": created_filter, "page": 1, "per_page": per_page},
                )
                workflow_runs += r["workflow_runs"]
                total_count = r["total_count"]
                logger.debug(f"{total_count=}")

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

                logger.info(f"Retrieved {len(workflow_runs)} workflows runs so far...")

        metadata = {
            "extraction_id": get_extraction_id(),
            "extracted_at": get_extracted_at(),
            "extracted_at_epoch": get_extracted_at_epoch(),
        }
        for workflow in workflow_runs:
            workflow = workflow.update(metadata)

        logger.info(f"Extracted data from {len(workflow_runs)} workflows runs from GitHub.")

        return workflow_runs

    def extract_github_jobs(
        self, workflow_runs: List[Dict[str, object]]
    ) -> List[Dict[str, object]]:
        logger.info("Extracting jobs data from GitHub...")

        jobs_data = []
        for workflow_run in workflow_runs:
            r = call_github_api(
                "GET",
                f"repos/pgoslatara/{workflow_run['repository']['name']}/actions/runs/{workflow_run['id']}/jobs",  # type: ignore[index]
            )
            if "message" not in r.keys():  # i.e. logs are still available
                for job in r["jobs"]:
                    jobs_data.append(job)

        metadata = {
            "extraction_id": get_extraction_id(),
            "extracted_at": get_extracted_at(),
            "extracted_at_epoch": get_extracted_at_epoch(),
        }
        for job in jobs_data:
            job = job.update(metadata)

        logger.info(f"Extracted data from {len(jobs_data)} jobs from GitHub.")

        return jobs_data

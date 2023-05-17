import logging
import os
import uuid
from datetime import datetime
from functools import lru_cache
from typing import Dict, List

import requests
from github import Github

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

        workflow_data = self.extract_github_workflow_runs()
        save_to_landing_zone(
            data=workflow_data,
            file_name=f"domain=github_actions_workflow_runs/schema_version=1/extracted_at={self.get_extracted_at_epoch()}/extraction_id={self.get_extraction_id()}.json",
        )

        jobs_data = self.extract_github_jobs(self.lookback_days, workflow_data)
        save_to_landing_zone(
            data=jobs_data,
            file_name=f"domain=github_actions_jobs/schema_version=1/extracted_at={self.get_extracted_at_epoch()}/extraction_id={self.get_extraction_id()}.json",
        )

    def extract_github_workflow_runs(self) -> List[Dict[str, object]]:
        logging.info("Extracting workflow data from GitHub...")
        workflow_data = []
        gh_client = Github(os.getenv("PAT_GITHUB"))
        for repo in gh_client.get_user().get_repos(type="owner"):
            logging.info(f"Extracting usage from {repo.name}...")
            for workflow_run in repo.get_workflow_runs():
                raw_data = workflow_run.raw_data
                try:
                    run_duration_ms = workflow_run.timing().run_duration_ms
                except AttributeError:
                    run_duration_ms = 0

                workflow_run_data = {
                    "extraction_id": self.get_extraction_id(),
                    "extracted_at": self.get_extracted_at(),
                    "extracted_at_epoch": self.get_extracted_at_epoch(),
                    "html_url": workflow_run.html_url,
                    "repo_name": repo.name,
                    "run_duration_ms": run_duration_ms,
                    "run_started_at": raw_data["run_started_at"],
                    "workflow_id": raw_data["workflow_id"],
                    "workflow_name": raw_data["name"],
                    "workflow_run_id": raw_data["id"],
                }
                workflow_data.append(workflow_run_data)

        logging.info(f"Extracted data from {len(workflow_data)} workflows from GitHub.")

        return workflow_data

    def extract_github_jobs(
        self, lookback_days: int, workflow_data: List[Dict[str, object]]
    ) -> List[Dict[str, object]]:
        logging.info(f"Extracting jobs data from GitHub (last {lookback_days} days)...")

        jobs_data = []
        for workflow in workflow_data:
            if (
                datetime.utcnow()
                - datetime.strptime(
                    str(workflow["run_started_at"]), "%Y-%m-%dT%H:%M:%SZ"
                )
            ).days <= lookback_days:
                r = requests.get(
                    f"https://api.github.com/repos/pgoslatara/medium_scraper/actions/runs/{workflow['workflow_run_id']}/jobs",
                    headers={
                        "Accept": "application/vnd.github+json",
                        "Authorization": f"Bearer {os.getenv('PAT_GITHUB')}",
                    },
                )
                if "message" not in r.json().keys():  # i.e. logs are still available
                    for job in r.json()["jobs"]:
                        job_data = {
                            "extraction_id": self.get_extraction_id(),
                            "extracted_at": self.get_extracted_at(),
                            "extracted_at_epoch": self.get_extracted_at_epoch(),
                            "completed_at": job["completed_at"],
                            "html_url": job["html_url"],
                            "job_id": job["id"],
                            "job_name": job["name"],
                            "started_at": job["started_at"],
                            "workflow_run_id": job["run_id"],
                        }
                        jobs_data.append(job_data)

                logging.info(f"Extracted data from {len(jobs_data)} jobs from GitHub.")

        return jobs_data

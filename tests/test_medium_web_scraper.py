"""Tests for MediumWebScraper behaviour when Medium blocks some or all tags."""

from typing import Dict, List

import pytest  # type: ignore[import-not-found]

from src.ingestion import medium_web_scraper
from src.ingestion.medium_web_scraper import MediumWebScraper
from utils.utils import MediumGraphQLError


def _patch_scraper(monkeypatch: pytest.MonkeyPatch, blocked_tags: List[str]) -> List[str]:
    """Fake scrape_blogs so blocked_tags raise, and record which tags were saved."""
    saved_files: List[str] = []

    def fake_scrape_blogs(self: MediumWebScraper, tag: str) -> List[Dict[str, object]]:
        if tag in blocked_tags:
            raise MediumGraphQLError("status 403")
        return [{"story_url": f"https://medium.com/{tag}", "tag": tag}]

    def fake_save_to_landing_zone(data: List[Dict[str, object]], file_name: str) -> None:
        saved_files.append(file_name)

    monkeypatch.setattr(MediumWebScraper, "scrape_blogs", fake_scrape_blogs)
    monkeypatch.setattr(MediumWebScraper, "scrape_authors", lambda self, extraction_id: [])
    monkeypatch.setattr(medium_web_scraper, "save_to_landing_zone", fake_save_to_landing_zone)
    return saved_files


def test_run_raises_when_every_tag_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    saved_files = _patch_scraper(monkeypatch, blocked_tags=["dbt", "databricks"])

    with pytest.raises(MediumGraphQLError) as exc_info:
        MediumWebScraper(tags=["dbt", "databricks"], lookback_days=7).run()

    assert "every tag" in str(exc_info.value)
    assert saved_files == []


def test_run_continues_when_some_tags_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    saved_files = _patch_scraper(monkeypatch, blocked_tags=["dbt"])

    MediumWebScraper(tags=["dbt", "databricks"], lookback_days=7).run()

    blog_files = [f for f in saved_files if f.startswith("domain=medium_blogs/")]
    assert len(blog_files) == 1
    assert "tag=databricks" in blog_files[0]


def test_run_succeeds_when_no_tags_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    saved_files = _patch_scraper(monkeypatch, blocked_tags=[])

    MediumWebScraper(tags=["dbt", "databricks"], lookback_days=7).run()

    assert len([f for f in saved_files if f.startswith("domain=medium_blogs/")]) == 2

"""Tests for paginating Medium's tag feed back to the start of the lookback window."""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytest  # type: ignore[import-not-found]

from src.ingestion.medium_web_scraper import MediumWebScraper


def _edge(published_at: datetime) -> Dict[str, Any]:
    return {
        "node": {
            "creator": {"name": "Author", "username": "author"},
            "extendedPreviewContent": {"subtitle": "-"},
            "firstPublishedAt": published_at.timestamp() * 1000,
            "mediumUrl": f"https://medium.com/p/{published_at.timestamp()}",
            "readingTime": 3,
            "title": "Title",
        }
    }


def _patch_feed(
    monkeypatch: pytest.MonkeyPatch, pages: List[List[datetime]]
) -> List[Optional[str]]:
    """Serve pages of posts (newest first) and record the cursor sent for each request."""
    cursors_requested: List[Optional[str]] = []

    def fake_fetch_tag_feed_page(
        self: MediumWebScraper, headers: Dict[str, str], tag: str, after: Optional[str]
    ) -> Dict[str, Any]:
        cursors_requested.append(after)
        page_index = len(cursors_requested) - 1
        return {
            "edges": [_edge(published_at) for published_at in pages[page_index]],
            "pageInfo": {
                "endCursor": f"cursor-{page_index + 1}",
                "hasNextPage": page_index + 1 < len(pages),
            },
        }

    monkeypatch.setattr(MediumWebScraper, "fetch_tag_feed_page", fake_fetch_tag_feed_page)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    return cursors_requested


def _days_ago(days: float) -> datetime:
    return datetime.now() - timedelta(days=days)


def test_scrape_blogs_pages_until_lookback_window_is_covered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursors_requested = _patch_feed(
        monkeypatch,
        pages=[
            [_days_ago(1), _days_ago(2)],
            [_days_ago(3), _days_ago(5)],
            [_days_ago(6), _days_ago(9)],
            [_days_ago(10), _days_ago(11)],
        ],
    )

    blogs = MediumWebScraper(tags=["dbt"], lookback_days=8).scrape_blogs(tag="dbt")

    # The third page reaches past the window, so the fourth is never requested.
    assert cursors_requested == [None, "cursor-1", "cursor-2"]
    assert len(blogs) == 6


def test_scrape_blogs_stops_when_feed_has_no_next_page(monkeypatch: pytest.MonkeyPatch) -> None:
    cursors_requested = _patch_feed(monkeypatch, pages=[[_days_ago(1)], [_days_ago(2)]])

    blogs = MediumWebScraper(tags=["dbt"], lookback_days=8).scrape_blogs(tag="dbt")

    assert cursors_requested == [None, "cursor-1"]
    assert len(blogs) == 2


def test_scrape_blogs_stops_at_page_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    cursors_requested = _patch_feed(monkeypatch, pages=[[_days_ago(0)]] * 30)
    scraper = MediumWebScraper(tags=["dbt"], lookback_days=8)

    blogs = scraper.scrape_blogs(tag="dbt")

    assert len(cursors_requested) == scraper.max_pages
    assert len(blogs) == scraper.max_pages

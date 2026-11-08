"""Tests for grouping the weekly Medium email by day and ranking by claps."""

import os
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest  # type: ignore[import-not-found]

from src.data_products.email_weekly_medium_blogs import SendMediumBlogsEmail


@pytest.fixture  # type: ignore[untyped-decorator]
def blogs_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Write a small fct_medium_blogs mart dated relative to today."""
    (tmp_path / "marts").mkdir()
    today = date.today()
    duckdb.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('Low', 5, TIMESTAMP '{today}' - INTERVAL 2 DAY + INTERVAL 9 HOUR, 'https://m/low', 'Low', '-', 3, 'dbt', FALSE),
                ('High', 900, TIMESTAMP '{today}' - INTERVAL 2 DAY + INTERVAL 10 HOUR, 'https://m/high', 'High', '-', 4, 'dbt', TRUE),
                ('Unknown', NULL, TIMESTAMP '{today}' - INTERVAL 2 DAY + INTERVAL 8 HOUR, 'https://m/unknown', 'Unknown', '-', 2, 'dbt', FALSE),
                ('Yesterday', 1, TIMESTAMP '{today}' - INTERVAL 1 DAY, 'https://m/yesterday', 'Yesterday', '-', 1, 'databricks', FALSE),
                ('Today', 10000, TIMESTAMP '{today}', 'https://m/today', 'Today', '-', 1, 'dbt', FALSE)
            ) AS t(author_name, clap_count, published_at, story_url, title, subtitle, reading_time_minutes, tag, is_author_based_in_netherlands)
        ) TO '{tmp_path / "marts" / "fct_medium_blogs.parquet"}' (FORMAT PARQUET)
    """)
    monkeypatch.setitem(os.environ, "DATA_DIR", str(tmp_path))
    return tmp_path


def test_get_relevant_blogs_ranks_by_claps_within_day(blogs_parquet: Path) -> None:
    blogs = SendMediumBlogsEmail(lookback_days=7).get_relevant_blogs()

    # Today's blog is excluded; nulls rank last within their day.
    assert [(b["author_name"], b["rank_within_day"]) for b in blogs] == [
        ("High", 1),
        ("Low", 2),
        ("Unknown", 3),
        ("Yesterday", 1),
    ]


def test_format_blogs_by_day_renders_one_section_per_day(blogs_parquet: Path) -> None:
    blogs = SendMediumBlogsEmail(lookback_days=7).get_relevant_blogs()

    html = SendMediumBlogsEmail.format_blogs_by_day(blogs)

    assert html.count("<table>") == 2
    assert "(3 blogs)" in html and "(1 blog)" in html
    assert html.index("https://m/high") < html.index("https://m/low")


def test_format_day_heading() -> None:
    assert SendMediumBlogsEmail.format_day_heading(datetime(2026, 11, 2).date()) == (
        "Monday 2 November 2026"
    )

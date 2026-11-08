import os
import smtplib
import ssl
from datetime import date
from email.message import EmailMessage
from itertools import groupby
from typing import Any, Dict, List

import duckdb
from tabulate import tabulate

from utils.logger import logger

EMAIL_COLUMNS = ["Rank", "Claps", "Author", "Blog", "Tag(s)"]


class SendMediumBlogsEmail:
    def __init__(self, lookback_days: int):
        self.lookback_days = lookback_days

    def get_relevant_blogs(self) -> List[Dict[str, Any]]:
        """Return blogs in the lookback window, ordered by day then by claps (highest first)."""
        df = duckdb.connect(database=":memory:").execute(f"""
            WITH base AS (
                SELECT
                    author_name,
                    clap_count,
                    published_at,
                    story_url,
                    CASE
                        WHEN subtitle = '-' THEN title || ' (' || reading_time_minutes || ' min.)'
                        ELSE title || ': ' || subtitle || ' (' || reading_time_minutes || ' min.)'
                    END
                    || IF(
                        is_author_based_in_netherlands = TRUE,
                        '🇳🇱',
                        ''
                    ) AS article_summary,
                    tag
                FROM read_parquet('{os.getenv('DATA_DIR')}/marts/fct_medium_blogs.parquet')
                WHERE
                    CAST(published_at AS DATE) >= (CURRENT_DATE() - INTERVAL {self.lookback_days} DAY)
                    AND CAST(published_at AS DATE) < CURRENT_DATE()
            )

            SELECT
                CAST(published_at AS DATE) AS publication_date,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(published_at AS DATE)
                    ORDER BY clap_count DESC NULLS LAST, published_at
                ) AS rank_within_day,
                clap_count,
                author_name,
                CASE
                    WHEN story_url = '-' THEN article_summary
                    ELSE '<a href="' || story_url || '">' || article_summary || '</a>'
                END AS blog_html,
                tag
            FROM base
            ORDER BY
                publication_date,
                rank_within_day
        """).arrow()

        blogs: List[Dict[str, Any]] = df.to_pylist()
        logger.debug(f"{blogs=}")
        logger.info(f"SELECTed {len(blogs)} blogs from the last {self.lookback_days} days.")
        return blogs

    @staticmethod
    def format_blogs_by_day(blogs: List[Dict[str, Any]]) -> str:
        """Render one HTML table per publication day. Expects blogs sorted by day then rank."""
        sections = []
        for publication_date, day_blogs in groupby(blogs, key=lambda b: b["publication_date"]):
            day_rows = list(day_blogs)
            blog_count_label = f"{len(day_rows)} blog{'' if len(day_rows) == 1 else 's'}"
            table = tabulate(
                [
                    [
                        blog["rank_within_day"],
                        # Older extractions pre-date clap capture
                        "-" if blog["clap_count"] is None else f"{blog['clap_count']:,}",
                        blog["author_name"],
                        blog["blog_html"],
                        blog["tag"],
                    ]
                    for blog in day_rows
                ],
                EMAIL_COLUMNS,
                tablefmt="unsafehtml",
            )
            sections.append(f"""
                <h3 style="font-family:Georgia, 'Times New Roman', Times, serif;color:#454349;">
                    {SendMediumBlogsEmail.format_day_heading(publication_date)} ({blog_count_label})
                </h3>
                {table}
                """)
        return "\n".join(sections)

    @staticmethod
    def format_day_heading(publication_date: date) -> str:
        return f"{publication_date:%A} {publication_date.day} {publication_date:%B %Y}"

    def run(self) -> None:
        blogs = self.get_relevant_blogs()
        logger.info("Assembling email...")

        sender_email_address = os.getenv("SENDER_EMAIL_ADDRESS")
        sender_email_password = os.getenv("SENDER_EMAIL_PASSWORD")
        recipient_email_address = os.getenv("RECIPIENT_EMAIL_ADDRESS")

        msg = EmailMessage()
        msg["Subject"] = "Relevant Medium Blogs"
        msg["From"] = sender_email_address
        msg["To"] = recipient_email_address

        formatted_blogs = self.format_blogs_by_day(blogs)
        msg.set_content(
            f"""
        <!DOCTYPE html>
        <html>
            <body>
                <div style="background-color:#eee;padding:10px 20px;">
                    <h2 style="font-family:Georgia, 'Times New Roman', Times, serif;color:#454349;">Medium blogs from the last {self.lookback_days} days</h2>
                    <p>Grouped by publication day, ranked by claps at time of scraping.</p>
                </div>
                <div style="padding:20px 0px">
                    {formatted_blogs}
                </div>
            </body>
        </html>
        """,
            subtype="html",
        )
        logger.debug(formatted_blogs)

        # In CI (pull_request runs) exercise everything up to the send but do not
        # dispatch — the placeholder recipient bounces back to the sender account.
        if os.getenv("CICD_RUN") == "True":
            logger.info("CICD_RUN is True, skipping email send.")
        elif sender_email_address and sender_email_password and recipient_email_address:
            logger.info("Sending email...")
            context = ssl.create_default_context()
            with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
                smtp.ehlo()
                smtp.starttls(context=context)
                smtp.ehlo()
                smtp.login(sender_email_address, sender_email_password)
                smtp.send_message(msg)
                logger.info("Email sent.")

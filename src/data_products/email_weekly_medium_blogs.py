import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Mapping, Union

import duckdb
from tabulate import tabulate

from utils.logger import logger


class SendMediumBlogsEmail:
    def __init__(self, lookback_days: int):
        self.lookback_days = lookback_days

    def get_relevant_blogs(self) -> Mapping[str, Union[str, int]]:
        df = (
            duckdb.connect(database=":memory:")
            .execute(
                f"""
            WITH base AS (
                SELECT
                    author_name,
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
                CAST(published_at AS DATE) AS 'Publication Date',
                author_name AS 'Author',
                CASE
                    WHEN story_url = '-' THEN article_summary
                    ELSE '<a href="' || story_url || '">' || article_summary || '</a>'
                END AS 'Blog',
                tag AS 'Tag(s)',
            FROM base
            ORDER BY published_at
        """
            )
            .arrow()
        )

        data = df.to_pydict()
        logger.debug(f"{data=}")
        logger.info(f"SELECTed {len(data['Blog'])} blogs from the last {self.lookback_days} days.")
        return dict(data)

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

        formatted_blogs = tabulate(
            list(map(list, zip(*[v for k, v in blogs.items()]))),
            blogs.keys(),  # type: ignore
            tablefmt="unsafehtml",
        )
        msg.set_content(
            f"""
        <!DOCTYPE html>
        <html>
            <body>
                <div style="background-color:#eee;padding:10px 20px;">
                    <h2 style="font-family:Georgia, 'Times New Roman', Times, serif;color#454349;">Medium blogs from the last {self.lookback_days} days</h2>
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

        if sender_email_address and sender_email_password and recipient_email_address:
            logger.info("Sending email...")
            context = ssl.create_default_context()
            with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
                smtp.ehlo()
                smtp.starttls(context=context)
                smtp.ehlo()
                smtp.login(sender_email_address, sender_email_password)
                smtp.send_message(msg)
                logger.info("Email sent.")

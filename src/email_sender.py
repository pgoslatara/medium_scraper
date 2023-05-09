import logging
import smtplib
import ssl
from email.message import EmailMessage

import pyarrow as pa
from tabulate import tabulate

from utils.utils import *


class EmailSender:
    def __init__(self, lookback_days: int):
        self.con = initialise_duckdb()
        self.lookback_days = lookback_days

    def get_relevant_blogs(self) -> dict:
        arrow_table = pa.Table.from_pylist(get_json_content())
        df = self.con.execute(
            f"""
            WITH base AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY story_url ORDER BY extracted_at_epoch DESC) AS row_num
                FROM arrow_table
                WHERE
                    CAST(published_at AS DATE) >= (CURRENT_DATE() - INTERVAL {self.lookback_days} DAY)
                    AND CAST(published_at AS DATE) < CURRENT_DATE()
            )

            , formatting AS (
                SELECT
                    author_name,
                    published_at,
                    story_url,
                    CASE
                        WHEN subtitle = '-' THEN title || ' (' || reading_time_minutes || ' min.)'
                        ELSE title || ': ' || subtitle || ' (' || reading_time_minutes || ' min.)'
                    END AS article_summary,
                    tag
                FROM base
                WHERE
                    row_num = 1
            )

            SELECT
                CAST(published_at AS DATE) AS 'Publication Date',
                author_name AS 'Author',
                CASE
                    WHEN story_url = '-' THEN article_summary
                    ELSE '<a href="' || story_url || '">' || article_summary || '</a>'
                END AS 'Blog',
                tag AS 'Tag(s)',
            FROM formatting
            ORDER BY published_at
        """
        ).arrow()

        data = df.to_pydict()
        logging.info(
            f"SELECTed {len(data)} blogs from the last {self.lookback_days} days."
        )
        return data

    def run(self):
        blogs = self.get_relevant_blogs()
        logging.info("Assembling email...")

        sender_email_address = os.getenv("SENDER_EMAIL_ADDRESS")
        sender_email_password = os.getenv("SENDER_EMAIL_PASSWORD")
        recipient_email_address = os.getenv("RECIPIENT_EMAIL_ADDRESS")

        msg = EmailMessage()
        msg["Subject"] = "Relevant Medium Blogs"
        msg["From"] = sender_email_address
        msg["To"] = recipient_email_address
        msg.set_content(
            f"""
        <!DOCTYPE html>
        <html>
            <body>
                <div style="background-color:#eee;padding:10px 20px;">
                    <h2 style="font-family:Georgia, 'Times New Roman', Times, serif;color#454349;">Medium blogs from the last {self.lookback_days} days</h2>
                </div>
                <div style="padding:20px 0px">
                    {tabulate(list(map(list, zip(*[v for k, v in blogs.items()]))), blogs.keys(), tablefmt="unsafehtml")}
                </div>
            </body>
        </html>
        """,
            subtype="html",
        )

        if sender_email_address and sender_email_password and recipient_email_address:
            logging.info("Sending email...")
            context = ssl.create_default_context()
            with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
                smtp.ehlo()
                smtp.starttls(context=context)
                smtp.ehlo()
                smtp.login(sender_email_address, sender_email_password)
                smtp.send_message(msg)
                logging.info("Email sent.")

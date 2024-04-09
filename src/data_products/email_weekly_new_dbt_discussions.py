import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Mapping, Union

import duckdb
from tabulate import tabulate

from utils.logger import logger


class SendNewDbtDiscussionEmail:
    def __init__(self, lookback_days: int):
        self.lookback_days = lookback_days

    def get_recent_discussions(self) -> Mapping[str, Union[str, int]]:
        df = (
            duckdb.connect(database=":memory:")
            .execute(
                f"""
                    SELECT
                        discussion_title,
                        discussion_url,
                        repo_name,
                        first_extracted_at,
                        created_at
                    FROM read_parquet("{os.getenv('DATA_DIR')}/marts/dim_dbt_discussions.parquet")
                    WHERE
                        created_at >= (GET_CURRENT_TIMESTAMP() - INTERVAL {self.lookback_days} DAY)
                        and repo_owner = 'dbt-labs'
                    ORDER BY first_extracted_at DESC
        """
            )
            .arrow()
        )

        data = df.to_pydict()
        logger.debug(f"{data=}")
        logger.info(
            f"SELECTed {len(data['discussion_url'])} discussions first extracted in the last {self.lookback_days} days."
        )
        return dict(data)

    def run(self) -> None:
        blogs = self.get_recent_discussions()
        logger.info("Assembling email...")

        sender_email_address = os.getenv("SENDER_EMAIL_ADDRESS")
        sender_email_password = os.getenv("SENDER_EMAIL_PASSWORD")
        recipient_email_address = os.getenv("RECIPIENT_EMAIL_ADDRESS")

        msg = EmailMessage()
        msg["Subject"] = "Recently created dbt discussions"
        msg["From"] = sender_email_address  # type: ignore
        msg["To"] = recipient_email_address  # type: ignore

        formatted_discussions = tabulate(
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
                    <h2 style="font-family:Georgia, 'Times New Roman', Times, serif;color#454349;">dbt discussions first extracted in the last {self.lookback_days} days</h2>
                </div>
                <div style="padding:20px 0px">
                    {formatted_discussions}
                </div>
            </body>
        </html>
        """,
            subtype="html",
        )
        logger.debug(formatted_discussions)
        logger.info(msg)

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

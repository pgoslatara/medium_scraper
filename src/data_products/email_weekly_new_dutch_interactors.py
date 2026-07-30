import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Mapping, Union

import duckdb
from tabulate import tabulate

from utils.logger import logger


class SendNewDutchInteractorEmail:
    def __init__(self, lookback_days: int):
        self.lookback_days = lookback_days

    def get_new_dutch_interactors(self) -> Mapping[str, Union[str, int]]:
        # first_interaction_at is the user's first ever issue or PR on a scraped repo, so
        # a recent value means the user is new to us rather than newly re-extracted.
        df = duckdb.connect(database=":memory:").execute(f"""
                    SELECT
                        username,
                        user_url,
                        location,
                        first_interaction_type,
                        first_interaction_repo,
                        first_interaction_title,
                        first_interaction_url,
                        first_interaction_at
                    FROM read_parquet("{os.getenv('DATA_DIR')}/marts/fct_dbt_interactors.parquet")
                    WHERE
                        is_user_based_in_netherlands
                        and first_interaction_at >= (GET_CURRENT_TIMESTAMP() - INTERVAL {self.lookback_days} DAY)
                    ORDER BY first_interaction_at DESC
        """).arrow()

        data = df.to_pydict()
        logger.debug(f"{data=}")
        logger.info(
            f"SELECTed {len(data['username'])} Netherlands-based users whose first issue or PR was created in the last {self.lookback_days} days."
        )
        return dict(data)

    def run(self) -> None:
        interactors = self.get_new_dutch_interactors()

        # Most weeks there are no new Netherlands-based interactors, so stay silent
        # rather than sending an email with an empty table.
        if not interactors["username"]:
            logger.info("No new Netherlands-based interactors, skipping email.")
            return

        logger.info("Assembling email...")

        sender_email_address = os.getenv("SENDER_EMAIL_ADDRESS")
        sender_email_password = os.getenv("SENDER_EMAIL_PASSWORD")
        recipient_email_address = os.getenv("RECIPIENT_EMAIL_ADDRESS")

        msg = EmailMessage()
        msg["Subject"] = "New Netherlands-based GitHub interactors"
        msg["From"] = sender_email_address
        msg["To"] = recipient_email_address

        formatted_interactors = tabulate(
            list(map(list, zip(*[v for k, v in interactors.items()]))),
            interactors.keys(),  # type: ignore
            tablefmt="unsafehtml",
        )
        msg.set_content(
            f"""
        <!DOCTYPE html>
        <html>
            <body>
                <div style="background-color:#eee;padding:10px 20px;">
                    <h2 style="font-family:Georgia, 'Times New Roman', Times, serif;color#454349;">Netherlands-based GitHub users whose first issue or PR was created in the last {self.lookback_days} days</h2>
                </div>
                <div style="padding:20px 0px">
                    {formatted_interactors}
                </div>
            </body>
        </html>
        """,
            subtype="html",
        )
        logger.debug(formatted_interactors)

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

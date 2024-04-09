import os
import smtplib
import ssl
from email.message import EmailMessage

import duckdb

from utils.logger import logger

df = duckdb.sql(
    f"""
    WITH base AS (
        SELECT
            CAST(run_started_at AS DATE) AS run_date,
            SUM(billable_minutes) AS daily_billable_minutes
        FROM "{os.getenv('DATA_DIR')}/marts/dim_github_action_usage.parquet"
        WHERE
            is_private_repo IS TRUE
            AND DATE_TRUNC('month', CAST(run_started_at AS DATE)) = DATE_TRUNC('month', TODAY())
        GROUP BY 1
    )
    , month_progress AS (
        SELECT
            EXTRACT(
                'EPOCH' FROM (CURRENT_TIMESTAMP - DATE_TRUNC('month', CURRENT_TIMESTAMP))
            ) /
            EXTRACT(
                'EPOCH' FROM (
                    (DATE_TRUNC('month', CURRENT_TIMESTAMP) + INTERVAL 1 MONTH)
                    -
                    DATE_TRUNC('month', CURRENT_TIMESTAMP)
                )
            ) AS pct_month_elapsed
    )

    SELECT
        run_date,
        daily_billable_minutes,
        SUM(daily_billable_minutes) OVER(ORDER BY run_date) AS monthly_billable_minutes,
        SUM(daily_billable_minutes) OVER(ORDER BY run_date) / 2000 AS monthly_billable_minutes_pct,
        pct_month_elapsed
    FROM base, month_progress
    GROUP BY 1,2,5
    ORDER BY 1 DESC
    LIMIT 1
"""
)

monthly_billable_minutes_pct = df.fetchall()[0][3]
pct_month_elapsed = df.fetchall()[0][4]
if monthly_billable_minutes_pct > pct_month_elapsed:
    # i.e. Percentage-wise, more Action minutes have been used than proportion of the month has elapsed
    sender_email_address = os.getenv("SENDER_EMAIL_ADDRESS")
    sender_email_password = os.getenv("SENDER_EMAIL_PASSWORD")
    recipient_email_address = os.getenv("RECIPIENT_EMAIL_ADDRESS")

    msg = EmailMessage()
    msg["Subject"] = "Relevant Medium Blogs"
    msg["From"] = sender_email_address  # type: ignore
    msg["To"] = recipient_email_address  # type: ignore

    msg.set_content(
        f"""
    <!DOCTYPE html>
    <html>
        <body>
            <div style="background-color:#eee;padding:10px 20px;">
                <h2 style="font-family:Georgia, 'Times New Roman', Times, serif;color#454349;">GitHub Actions usage alert</h2>
            </div>
            <div style="padding:20px 0px">Percentage-wise, more Action minutes have been used than proportion of the moonth has elapsed</div>
            <br></br>
            <div style="padding:20px 0px">% Action minutes used: {monthly_billable_minutes_pct*100:.0f}%</div>
            <div style="padding:20px 0px">% month elapsed: {pct_month_elapsed*100:.0f}%</div>
            <br></br>
            <a href="https://github.com/settings/billing#:~:text=Get%20usage%20report-,Actions,-Included%20minutes%20quota">GitHub Actions Usage</a>
        </body>
    </html>
    """,
        subtype="html",
    )
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

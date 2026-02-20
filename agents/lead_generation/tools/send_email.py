"""
Stage 5: Email Delivery via Gmail SMTP.
Sends the daily email with the Excel attachment.
"""

import logging
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from tools.config import GMAIL_ADDRESS, GMAIL_APP_PASSWORD, RECIPIENT_EMAIL
from tools.utils import retry

logger = logging.getLogger("pipeline")

SMTP_HOST = "smtp.gmail.com"
SMTP_SSL_PORT = 465
SMTP_STARTTLS_PORT = 587
SMTP_TIMEOUT = 30


@retry(max_attempts=3, backoff_factor=5, exceptions=(smtplib.SMTPException, OSError))
def _send_via_smtp_ssl(recipient, subject, html_body, attachment_path=None):
    """Send an email via Gmail SMTP over SSL (port 465)."""
    msg = MIMEMultipart("mixed")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = recipient
    msg["Subject"] = subject

    # HTML body
    html_part = MIMEText(html_body, "html", "utf-8")
    msg.attach(html_part)

    # Excel attachment
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            attachment = MIMEApplication(f.read(), _subtype="xlsx")
            filename = os.path.basename(attachment_path)
            attachment.add_header("Content-Disposition", "attachment", filename=filename)
            msg.attach(attachment)
        logger.debug(f"Attached: {attachment_path}")

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_SSL_PORT, timeout=SMTP_TIMEOUT) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.send_message(msg)

    logger.info(f"Email sent to {recipient}")


@retry(max_attempts=2, backoff_factor=3, exceptions=(smtplib.SMTPException, OSError))
def _send_via_smtp_starttls(recipient, subject, html_body, attachment_path=None):
    """Fallback path via SMTP + STARTTLS (port 587)."""
    msg = MIMEMultipart("mixed")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            attachment = MIMEApplication(f.read(), _subtype="xlsx")
            filename = os.path.basename(attachment_path)
            attachment.add_header("Content-Disposition", "attachment", filename=filename)
            msg.attach(attachment)
        logger.debug(f"Attached: {attachment_path}")

    with smtplib.SMTP(SMTP_HOST, SMTP_STARTTLS_PORT, timeout=SMTP_TIMEOUT) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.send_message(msg)

    logger.info(f"Email sent to {recipient} via STARTTLS fallback")


def run(recipient=None, subject="Informe Diario de Leads", html_body="", attachment_path=None):
    """
    Send the daily lead report email.

    Args:
        recipient: Email address to send to (defaults to RECIPIENT_EMAIL or GMAIL_ADDRESS)
        subject: Email subject line
        html_body: HTML content for the email body
        attachment_path: Path to the Excel file to attach
    """
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        logger.error("Gmail credentials not configured. Cannot send email.")
        return False

    if not recipient:
        recipient = RECIPIENT_EMAIL or GMAIL_ADDRESS

    if not recipient:
        logger.error("No recipient email configured.")
        return False

    logger.info(f"Sending email to {recipient}...")
    logger.info(f"Subject: {subject}")

    try:
        _send_via_smtp_ssl(recipient, subject, html_body, attachment_path)
        return True
    except Exception as ssl_error:
        logger.warning(f"SSL SMTP failed ({SMTP_SSL_PORT}): {ssl_error}. Trying STARTTLS...")
        try:
            _send_via_smtp_starttls(recipient, subject, html_body, attachment_path)
            return True
        except Exception as tls_error:
            logger.error(f"Failed to send email (SSL + STARTTLS): {tls_error}")
            return False


if __name__ == "__main__":
    from tools.utils import setup_logging

    setup_logging()

    # Send a test email
    test_html = """
    <html><body>
    <h2>Test Email</h2>
    <p>If you see this, the Gmail SMTP configuration is working correctly.</p>
    <p>Pipeline: Lead Generation & Enrichment</p>
    </body></html>
    """
    success = run(subject="[TEST] Pipeline Email Configuration", html_body=test_html)
    print(f"Test email sent: {success}")

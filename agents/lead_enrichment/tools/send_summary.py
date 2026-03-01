"""
send_summary.py
---------------
Sends a brief plain-text summary email to the user after a campaign run.
Includes a link to today's Google Sheet if available.

Usage:
    python tools/send_summary.py

Env vars required:
    GMAIL_USER        - sender address
    GMAIL_PASSWORD    - Gmail App Password
    SUMMARY_EMAIL     - where to send the summary (usually same as GMAIL_USER)
"""

import csv
import os
import smtplib
import sys
from collections import Counter
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

LOG_FILE = Path(".tmp/sent_log.csv")
SHEET_URL_FILE = Path(".tmp/sheet_url.txt")


def load_log() -> list[dict]:
    if not LOG_FILE.exists():
        print(f"ERROR: {LOG_FILE} not found. Run send_emails.py first.")
        sys.exit(1)
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_summary(rows: list[dict], sheet_url: str) -> str:
    sent = [r for r in rows if r.get("status") == "sent"]
    errors = [r for r in rows if r.get("status", "").startswith("error")]
    by_country = Counter(r.get("country", "?") for r in sent)

    lines = [
        f"Resumen campaña — {datetime.now().strftime('%d/%m/%Y')}",
        "",
        f"✓ Enviados:  {len(sent)}",
        f"✗ Errores:   {len(errors)}",
        "",
        "Por país:",
    ]
    for country, count in sorted(by_country.items(), key=lambda x: -x[1]):
        lines.append(f"  {country}: {count}")

    if sheet_url:
        lines += ["", f"Google Sheet del día: {sheet_url}"]

    if errors:
        lines += ["", "Errores:"]
        for e in errors:
            lines.append(f"  {e.get('email')} — {e.get('status')}")

    lines += ["", "—", "Sistema de Lead Enrichment"]
    return "\n".join(lines)


def main():
    gmail_user = os.getenv("GMAIL_USER")
    gmail_password = os.getenv("GMAIL_PASSWORD")
    summary_email = os.getenv("SUMMARY_EMAIL") or gmail_user

    if not gmail_user or not gmail_password:
        print("ERROR: GMAIL_USER and GMAIL_PASSWORD must be set in .env")
        sys.exit(1)

    rows = load_log()

    sheet_url = ""
    if SHEET_URL_FILE.exists():
        sheet_url = SHEET_URL_FILE.read_text(encoding="utf-8").strip()

    body = build_summary(rows, sheet_url)

    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = gmail_user
    msg["To"] = summary_email
    msg["Subject"] = f"Campaña {datetime.now().strftime('%d/%m/%Y')} — {sum(1 for r in rows if r.get('status') == 'sent')} enviados"

    smtp = smtplib.SMTP("smtp.gmail.com", 587)
    smtp.ehlo()
    smtp.starttls()
    smtp.login(gmail_user, gmail_password)
    smtp.sendmail(gmail_user, summary_email, msg.as_string())
    smtp.quit()

    print(f"Summary sent to {summary_email}")
    print()
    print(body)


if __name__ == "__main__":
    main()

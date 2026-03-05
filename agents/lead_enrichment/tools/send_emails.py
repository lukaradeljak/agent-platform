"""
send_emails.py
--------------
Reads .tmp/leads.csv, applies the email template, and sends one email
per lead via Gmail SMTP, with a 3-minute delay between sends.

Usage:
    python tools/send_emails.py             # live send
    python tools/send_emails.py --dry-run   # preview only, no sending

Env vars required:
    GMAIL_USER         - your Gmail address (e.g. you@yourdomain.com)
    GMAIL_PASSWORD     - Gmail App Password (NOT your regular password)
    EMAIL_SUBJECT      - Subject line (can include {{empresa}})
    EMAIL_SIGNATURE    - Your signature block (plain text, multiline OK)
"""

import argparse
import csv
import os
import smtplib
import sys
import time
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TMP_DIR = Path(".tmp")
LEADS_FILE = TMP_DIR / "leads.csv"
LOG_FILE = TMP_DIR / "sent_log.csv"

DELAY_SECONDS = 3 * 60  # 3 minutes between sends

EMAIL_TEMPLATE = """\
{nombre},

Hay un cambio operativo en marcha que está separando a las empresas que van a escalar de las que no. Vale la pena tenerlo en cuenta.

JP Morgan automatizó con IA lo que antes le consumía 360.000 horas de trabajo legal al año. No son los únicos, y no son los más grandes que lo están haciendo. Empresas de todos los tamaños están automatizando capas enteras de su operación: coordinación, reportes, seguimiento, decisiones de rutina. El resultado es operar al doble de velocidad con el mismo equipo y un ROI que aparece en los primeros dos meses.

Lo que hoy le consume horas a su equipo ya no tiene por qué depender de nadie. Nosotros implementamos eso con la tecnología más avanzada disponible hoy: sistemas completamente a medida, construidos sobre LangGraph, la infraestructura de IA que usan las organizaciones más eficientes del mundo.

Para implementarlo bien, la tecnología y quién lo ejecuta importan. En eso nos especializamos. Puede ver más en https://acemsystems.com

{firma}"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_body(first_name: str, company: str, signature: str) -> str:
    return EMAIL_TEMPLATE.format(
        nombre=first_name,
        empresa=company,
        firma=signature,
    )


def build_subject(template: str, company: str) -> str:
    return template.replace("{{empresa}}", company)


def send_email(smtp: smtplib.SMTP, sender: str, recipient: str,
               subject: str, body: str):
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    smtp.sendmail(sender, recipient, msg.as_string())


def init_log():
    if not LOG_FILE.exists():
        TMP_DIR.mkdir(exist_ok=True)
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["company", "name", "email", "phone", "country", "sent_at", "status"])
            writer.writeheader()


def append_log(row: dict):
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["company", "name", "email", "phone", "country", "sent_at", "status"])
        writer.writerow(row)


def load_already_sent() -> set:
    if not LOG_FILE.exists():
        return set()
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["email"] for row in reader if row.get("status") == "sent"}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Send emails to leads.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview emails without sending them.")
    parser.add_argument("--preview-to", type=str, default=None,
                        help="Send all emails to this address instead of the real leads (for testing).")
    args = parser.parse_args()

    # --- Load env vars ---
    gmail_user = os.getenv("GMAIL_USER")
    gmail_password = os.getenv("GMAIL_PASSWORD")
    subject_template = os.getenv("EMAIL_SUBJECT", "Automatización que reduce costos en {{empresa}}")
    signature = os.getenv("EMAIL_SIGNATURE", "")

    if not gmail_user or not gmail_password:
        print("ERROR: GMAIL_USER and GMAIL_PASSWORD must be set in .env")
        sys.exit(1)

    if not signature:
        print("WARNING: EMAIL_SIGNATURE is empty. Add your name/contact to .env")

    # --- Load leads ---
    if not LEADS_FILE.exists():
        print(f"ERROR: {LEADS_FILE} not found. Run apollo_search.py first.")
        sys.exit(1)

    with open(LEADS_FILE, "r", encoding="utf-8") as f:
        leads = list(csv.DictReader(f))

    if not leads:
        print("No leads found in leads.csv.")
        sys.exit(0)

    # --- Skip already sent ---
    already_sent = load_already_sent()
    pending = [l for l in leads if l["email"] not in already_sent]
    skipped = len(leads) - len(pending)
    if skipped:
        print(f"Skipping {skipped} already-sent lead(s).")

    if not pending:
        print("All leads already sent. Nothing to do.")
        sys.exit(0)

    init_log()

    if args.preview_to:
        print(f"PREVIEW MODE: all emails will be sent to {args.preview_to} (not to leads)\n")

    # --- Connect to Gmail SMTP ---
    smtp = None
    if not args.dry_run:
        print(f"Connecting to Gmail as {gmail_user}...")
        smtp = smtplib.SMTP("smtp.gmail.com", 587)
        smtp.ehlo()
        smtp.starttls()
        smtp.login(gmail_user, gmail_password)
        print("Connected.\n")

    # --- Send / preview ---
    for i, lead in enumerate(pending, start=1):
        first_name = lead.get("first_name", "").strip() or "Hola"
        company = lead.get("company", "").strip() or "su empresa"
        email = lead["email"].strip()
        country = lead.get("country", "")
        full_name = f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()

        subject = build_subject(subject_template, company)
        body = build_body(first_name, company, signature)

        print(f"[{i}/{len(pending)}] -> {full_name} <{email}> | {company} | {country}")
        if args.dry_run:
            print(f"  Subject: {subject}")
            print(f"  Body preview: {body[:120].strip()}...")
            print()
            continue

        status = "sent"
        actual_recipient = args.preview_to if args.preview_to else email
        try:
            send_email(smtp, gmail_user, actual_recipient, subject, body)
            print(f"  OK Sent -> {actual_recipient}")
        except Exception as e:
            status = f"error: {e}"
            print(f"  FAILED: {e}")

        if not args.preview_to:
            append_log({
                "company": company,
                "name": full_name,
                "email": email,
                "phone": lead.get("phone", ""),
                "country": country,
                "sent_at": datetime.now().isoformat(timespec="seconds"),
                "status": status,
            })

        if i < len(pending) and not args.preview_to:
            print(f"  Waiting 3 minutes before next send...\n")
            time.sleep(DELAY_SECONDS)

    if smtp:
        smtp.quit()

    total_sent = sum(1 for l in pending if l["email"] not in already_sent)
    print(f"\nDone. Log saved -> {LOG_FILE}")


if __name__ == "__main__":
    main()

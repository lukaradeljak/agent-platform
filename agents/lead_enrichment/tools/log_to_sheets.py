"""
log_to_sheets.py
----------------
Creates a NEW Google Sheet for today's campaign run and writes the sent log.
Sheet name: "Campaña YYYY-MM-DD"

Uses OAuth 2.0 (client_secrets.json) — only requires Sheets API (no Drive API needed).
First run opens a browser for authorization and saves token.json.

The URL of the new sheet is saved to .tmp/sheet_url.txt so send_summary.py
can include it in the summary email.

Usage:
    python tools/log_to_sheets.py

Files required:
    client_secrets.json  - OAuth client credentials (project root)
    token.json           - Auto-created after first auth (project root)
"""

import csv
import sys
from datetime import datetime
from pathlib import Path

import gspread
import requests as http_requests
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv()

LOG_FILE = Path(".tmp/current_run_log.csv")
SHEET_URL_FILE = Path(".tmp/sheet_url.txt")
MASTER_SHEET_ID_FILE = Path(".tmp/master_sheet_id.txt")
CLIENT_SECRETS_FILE = "client_secrets.json"
TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
HEADERS = ["Empresa", "Nombre", "Email", "Pais", "Telefono", "Fecha de envio", "Estado"]
CSV_FIELDS = ["company", "name", "email", "country", "phone", "sent_at", "status"]


def get_credentials() -> Credentials:
    creds = None

    if Path(TOKEN_FILE).exists():
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not Path(CLIENT_SECRETS_FILE).exists():
                print(f"ERROR: {CLIENT_SECRETS_FILE} not found.")
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return creds


def create_spreadsheet(creds: Credentials, title: str) -> tuple[str, str]:
    """Create a new spreadsheet via Sheets API v4 (no Drive API required).
    Returns (spreadsheet_id, spreadsheet_url).
    """
    resp = http_requests.post(
        "https://sheets.googleapis.com/v4/spreadsheets",
        headers={"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"},
        json={"properties": {"title": title}},
    )
    if resp.status_code != 200:
        print(f"ERROR creando spreadsheet: {resp.status_code} {resp.text}")
        sys.exit(1)

    data = resp.json()
    sheet_id = data["spreadsheetId"]
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    return sheet_id, sheet_url


def load_log() -> list[list]:
    if not LOG_FILE.exists():
        print(f"ERROR: {LOG_FILE} not found. Run send_emails.py first.")
        sys.exit(1)
    rows = []
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append([row.get(f, "") for f in CSV_FIELDS])
    return rows


def append_to_master_sheet(creds: Credentials, rows: list[list]):
    """Append rows to the persistent master sheet (historical log).
    Creates it on first run and saves its ID for future appends.
    """
    gc = gspread.authorize(creds)

    if MASTER_SHEET_ID_FILE.exists():
        master_id = MASTER_SHEET_ID_FILE.read_text(encoding="utf-8").strip()
        try:
            spreadsheet = gc.open_by_key(master_id)
            ws = spreadsheet.sheet1
            ws.append_rows(rows, value_input_option="RAW")
            print(f"Historico: {len(rows)} fila(s) agregadas -> {spreadsheet.url}")
            return
        except Exception as e:
            print(f"No se pudo abrir master sheet ({master_id}): {e}. Creando uno nuevo...")

    master_id, master_url = create_spreadsheet(creds, "Lead Enrichment - Historico")
    spreadsheet = gc.open_by_key(master_id)
    ws = spreadsheet.sheet1
    ws.update_title("Historico")
    ws.update([HEADERS] + rows, value_input_option="RAW")
    ws.format("A1:G1", {"textFormat": {"bold": True}})

    Path(".tmp").mkdir(exist_ok=True)
    MASTER_SHEET_ID_FILE.write_text(master_id, encoding="utf-8")
    print(f"Historico creado: {master_url}")
    print(f"ID guardado -> {MASTER_SHEET_ID_FILE}")


def main():
    rows = load_log()
    if not rows:
        print("No rows in sent_log.csv - nothing to log.")
        sys.exit(0)

    creds = get_credentials()

    # --- Sheet diario ---
    today = datetime.now().strftime("%Y-%m-%d")
    title = f"Campana {today}"

    print(f"Creando sheet '{title}'...")
    sheet_id, sheet_url = create_spreadsheet(creds, title)
    print(f"URL: {sheet_url}")

    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(sheet_id)
    ws = spreadsheet.sheet1
    ws.update_title("Enviados")

    ws.update([HEADERS] + rows, value_input_option="RAW")
    ws.format("A1:G1", {"textFormat": {"bold": True}})

    Path(".tmp").mkdir(exist_ok=True)
    SHEET_URL_FILE.write_text(sheet_url, encoding="utf-8")

    print(f"Registradas {len(rows)} fila(s).")
    print(f"URL guardada -> {SHEET_URL_FILE}")

    # --- Sheet histórico (append) ---
    try:
        append_to_master_sheet(creds, rows)
    except Exception as e:
        print(f"WARNING: No se pudo actualizar sheet historico: {e}")


if __name__ == "__main__":
    main()

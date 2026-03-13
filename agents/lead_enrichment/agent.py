"""
Lead Enrichment & Email Sender Agent.

Busca decision-makers en Apollo.io (CEO, COO, etc.) en empresas LATAM/España
de 25-200 empleados, y les envía cold emails via Gmail.

Corre lunes a viernes a las 09:00 ART (12:00 UTC).
"""

import os
import smtplib
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from celery.schedules import crontab
from dotenv import load_dotenv

from agents.base_agent import BaseAgent

_AGENT_DIR = os.path.dirname(__file__)
_TOOLS_DIR = os.path.join(_AGENT_DIR, "tools")

# Cada agente usa su propia subcarpeta en /app/.tmp para evitar conflictos
# cuando hay múltiples agentes corriendo en el mismo servidor.
_TMP_DIR = Path("/app/.tmp/lead_enrichment")


class LeadEnrichmentAgent(BaseAgent):
    name = "lead_enrichment"
    # 09:00 ART (UTC-3) = 12:00 UTC, lunes a viernes
    schedule = crontab(hour=12, minute=0, day_of_week="1-5")

    def run(self) -> dict[str, Any]:
        # Cargar secrets del agente
        load_dotenv(os.path.join(_AGENT_DIR, ".env"), override=True)

        dry_run = os.getenv("DRY_RUN", "").lower() in ("1", "true", "yes")

        if _TOOLS_DIR not in sys.path:
            sys.path.insert(0, _TOOLS_DIR)

        # ── Guard: una sola ejecución por día ─────────────────────────────
        _TMP_DIR.mkdir(parents=True, exist_ok=True)
        today_str = datetime.now().strftime("%Y-%m-%d")
        done_file = _TMP_DIR / f"{today_str}.done"
        if not dry_run and done_file.exists():
            return {"resultados": 0, "errores": 0}
        if not dry_run:
            done_file.touch()

        import apollo_search as apollo
        import send_emails as emailer

        # Redirigir todos los paths de los tools a la subcarpeta del agente
        apollo.TMP_DIR = _TMP_DIR
        apollo.OUTPUT_FILE = _TMP_DIR / "leads.csv"
        apollo.ROTATION_STATE_FILE = _TMP_DIR / "country_rotation.json"
        emailer.TMP_DIR = _TMP_DIR
        emailer.LEADS_FILE = _TMP_DIR / "leads.csv"
        emailer.LOG_FILE = _TMP_DIR / "sent_log.csv"
        emailer.CURRENT_RUN_LOG = _TMP_DIR / "current_run_log.csv"

        api_key = os.getenv("APOLLO_API_KEY", "")
        gmail_user = os.getenv("GMAIL_USER", "")
        gmail_password = os.getenv("GMAIL_PASSWORD", "")
        subject_template = os.getenv("EMAIL_SUBJECT", "Automatizacion que reduce costos en {{empresa}}")
        signature = os.getenv("EMAIL_SIGNATURE", "")

        # ── Paso 1: Buscar leads en Apollo ────────────────────────────────
        country = apollo.get_next_country(None)
        leads = apollo.search_people(api_key, country, 40)
        apollo.save_leads(leads)

        if not leads:
            return {"resultados": 0, "errores": 0}

        # ── Paso 2: Enviar emails ─────────────────────────────────────────
        already_sent = emailer.load_already_sent()
        pending = [lead for lead in leads if lead["email"] not in already_sent]
        emailer.init_log()
        emailer.init_current_run_log()

        emails_sent = 0
        errors_count = 0

        for i, lead in enumerate(pending):
            first_name = lead.get("first_name", "").strip() or "Hola"
            company = lead.get("company", "").strip() or "su empresa"
            email = lead["email"].strip()
            full_name = f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()

            subject = emailer.build_subject(subject_template, company)
            body = emailer.build_body(first_name, company, signature)

            if dry_run:
                status = "dry_run"
                emails_sent += 1
            else:
                status = "sent"
                try:
                    smtp = smtplib.SMTP("smtp.gmail.com", 587)
                    smtp.ehlo()
                    smtp.starttls()
                    smtp.login(gmail_user, gmail_password)
                    try:
                        emailer.send_email(smtp, gmail_user, email, subject, body)
                        emails_sent += 1
                    finally:
                        smtp.quit()
                except Exception as exc:
                    status = f"error: {exc}"
                    errors_count += 1

            log_row = {
                "company": company,
                "name": full_name,
                "email": email,
                "country": lead.get("country", ""),
                "phone": lead.get("phone", ""),
                "sent_at": datetime.now().isoformat(timespec="seconds"),
                "status": status,
            }
            emailer.append_log(log_row)
            emailer.append_current_run_log(log_row)

            if i < len(pending) - 1:
                time.sleep(emailer.DELAY_SECONDS)

        # ── Paso 3: Google Sheets ─────────────────────────────────────────
        sheet_url = ""
        try:
            import shutil
            for cred_file in ("token.json", "client_secrets.json"):
                src = Path(_AGENT_DIR) / cred_file
                dst = _TMP_DIR / cred_file
                if src.exists():
                    shutil.copy2(src, dst)

            original_cwd = os.getcwd()
            os.chdir(_TMP_DIR)
            try:
                import log_to_sheets
                log_to_sheets.LOG_FILE = _TMP_DIR / "current_run_log.csv"
                log_to_sheets.SHEET_URL_FILE = _TMP_DIR / "sheet_url.txt"
                log_to_sheets.MASTER_SHEET_ID_FILE = _TMP_DIR / "master_sheet_id.txt"
                log_to_sheets.main()
                url_file = _TMP_DIR / "sheet_url.txt"
                if url_file.exists():
                    sheet_url = url_file.read_text(encoding="utf-8").strip()
                refreshed = _TMP_DIR / "token.json"
                if refreshed.exists():
                    try:
                        shutil.copy2(refreshed, Path(_AGENT_DIR) / "token.json")
                    except Exception:
                        pass
            finally:
                os.chdir(original_cwd)
        except (SystemExit, Exception):
            pass

        # ── Paso 4: Resumen por email ─────────────────────────────────────
        try:
            import send_summary
            send_summary.LOG_FILE = _TMP_DIR / "current_run_log.csv"
            send_summary.SHEET_URL_FILE = _TMP_DIR / "sheet_url.txt"
            send_summary.main()
        except (SystemExit, Exception):
            pass

        return {
            "resultados": emails_sent,
            "errores": errors_count,
        }

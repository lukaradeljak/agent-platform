"""
Lead Enrichment & Email Sender Agent.

Busca decision-makers en Apollo.io (CEO, COO, etc.) en empresas LATAM/España
de 25-200 empleados, y les envía cold emails via Gmail.

Corre lunes a viernes a las 09:00 ART (12:00 UTC).
"""

import csv
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

_TMP_DIR = Path("/app/.tmp")


class LeadEnrichmentAgent(BaseAgent):
    name = "lead_enrichment"
    # 09:00 ART (UTC-3) = 12:00 UTC, lunes a viernes
    schedule = crontab(hour=12, minute=0, day_of_week="1-5")

    def run(self) -> dict[str, Any]:
        # Cargar secrets del agente
        load_dotenv(os.path.join(_AGENT_DIR, ".env"), override=True)

        if _TOOLS_DIR not in sys.path:
            sys.path.insert(0, _TOOLS_DIR)

        # ── Guard: una sola ejecución por día ─────────────────────────────
        # Celery tiene max_retries=3. Sin este guard, si el agente falla a
        # mitad de la campaña (error SMTP, rate limit), el reintento buscaría
        # 40 leads NUEVOS en Apollo y mandaría emails adicionales.
        # El .done file previene eso: si ya corrió hoy, salir de inmediato.
        _TMP_DIR.mkdir(exist_ok=True)
        today_str = datetime.now().strftime("%Y-%m-%d")
        done_file = _TMP_DIR / f"lead_enrichment_{today_str}.done"
        if done_file.exists():
            return {"resultados": 0, "errores": 0}
        # Marcar ANTES de cualquier operación con efectos secundarios
        done_file.touch()

        import apollo_search as apollo
        import send_emails as emailer

        api_key = os.getenv("APOLLO_API_KEY", "")
        gmail_user = os.getenv("GMAIL_USER", "")
        gmail_password = os.getenv("GMAIL_PASSWORD", "")
        subject_template = os.getenv("EMAIL_SUBJECT", "Automatizacion que reduce costos en {{empresa}}")
        signature = os.getenv("EMAIL_SIGNATURE", "")

        # ── Paso 1: Buscar leads en Apollo ────────────────────────────────
        country = apollo.get_next_country(None)
        leads = apollo.search_people(api_key, country, 40)
        apollo.save_leads(leads)
        leads_found = len(leads)

        if not leads_found:
            return {
                "resultados": 0,
                "errores": 0,
            }

        # ── Paso 2: Enviar emails ─────────────────────────────────────────
        already_sent = emailer.load_already_sent()
        pending = [lead for lead in leads if lead["email"] not in already_sent]
        emailer.init_log()

        emails_sent = 0
        errors_count = 0

        smtp = smtplib.SMTP("smtp.gmail.com", 587)
        smtp.ehlo()
        smtp.starttls()
        smtp.login(gmail_user, gmail_password)

        for i, lead in enumerate(pending):
            first_name = lead.get("first_name", "").strip() or "Hola"
            company = lead.get("company", "").strip() or "su empresa"
            email = lead["email"].strip()
            full_name = f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()

            subject = emailer.build_subject(subject_template, company)
            body = emailer.build_body(first_name, company, signature)

            status = "sent"
            try:
                emailer.send_email(smtp, gmail_user, email, subject, body)
                emails_sent += 1
            except Exception as exc:
                status = f"error: {exc}"
                errors_count += 1

            emailer.append_log({
                "company": company,
                "name": full_name,
                "email": email,
                "country": lead.get("country", ""),
                "sent_at": datetime.now().isoformat(timespec="seconds"),
                "status": status,
            })

            # Delay de 3 minutos entre emails (protege reputación del dominio)
            if i < len(pending) - 1:
                time.sleep(emailer.DELAY_SECONDS)

        smtp.quit()

        # ── Paso 3: Google Sheets ─────────────────────────────────────────
        # Copia las credenciales OAuth al directorio temporal (writable) para
        # que log_to_sheets pueda leer/escribir token.json al refrescarlo.
        # Cambia CWD a /app/.tmp para que log_to_sheets encuentre token.json y
        # client_secrets.json, pero monkeypatea LOG_FILE y SHEET_URL_FILE para
        # que usen rutas absolutas (evita el bug de /app/.tmp/.tmp/...).
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
                log_to_sheets.LOG_FILE = _TMP_DIR / "sent_log.csv"
                log_to_sheets.SHEET_URL_FILE = _TMP_DIR / "sheet_url.txt"
                log_to_sheets.main()
                url_file = _TMP_DIR / "sheet_url.txt"
                if url_file.exists():
                    sheet_url = url_file.read_text(encoding="utf-8").strip()
                # Sincroniza token.json actualizado de vuelta al directorio del agente
                refreshed = _TMP_DIR / "token.json"
                if refreshed.exists():
                    try:
                        shutil.copy2(refreshed, Path(_AGENT_DIR) / "token.json")
                    except Exception:
                        pass  # agents dir es read-only en prod, ignorar
            finally:
                os.chdir(original_cwd)
        except (SystemExit, Exception):
            pass  # Sheets es best-effort, no falla el agente

        # ── Paso 4: Resumen por email ─────────────────────────────────────
        # send_summary no necesita credenciales de Google — no cambiar CWD.
        # Monkeypatea LOG_FILE y SHEET_URL_FILE con rutas absolutas para que
        # encuentre los archivos correctos en /app/.tmp/.
        try:
            import send_summary
            send_summary.LOG_FILE = _TMP_DIR / "sent_log.csv"
            send_summary.SHEET_URL_FILE = _TMP_DIR / "sheet_url.txt"
            send_summary.main()
        except (SystemExit, Exception):
            pass  # El resumen es best-effort, no falla el agente

        return {
            "resultados": emails_sent,
            "errores": errors_count,
        }

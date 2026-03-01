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


class LeadEnrichmentAgent(BaseAgent):
    name = "lead_enrichment"
    # 09:00 ART (UTC-3) = 12:00 UTC, lunes a viernes
    schedule = crontab(hour=12, minute=0, day_of_week="1-5")

    def run(self) -> dict[str, Any]:
        # Cargar secrets del agente
        load_dotenv(os.path.join(_AGENT_DIR, ".env"), override=True)

        if _TOOLS_DIR not in sys.path:
            sys.path.insert(0, _TOOLS_DIR)

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
                "leads_encontrados": 0,
                "emails_enviados": 0,
                "errores_email": 0,
                "pais": country,
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

        # ── Paso 3: Resumen por email ─────────────────────────────────────
        try:
            import send_summary
            send_summary.main()
        except (SystemExit, Exception):
            pass  # El resumen es best-effort, no falla el agente

        return {
            "leads_encontrados": leads_found,
            "emails_enviados": emails_sent,
            "errores_email": errors_count,
            "pais": country,
        }

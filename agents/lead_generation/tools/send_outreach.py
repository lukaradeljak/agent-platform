"""
Stage 6: Personalized Outreach.
Sends individual, AI-generated emails to each lead via GMass.
"""

import logging
import time

from tools.config import AI_REQUEST_DELAY, GMASS_API_KEY, LEADS_PER_DAY, OUTREACH_TRANSPORT
from tools.db_manager import (
    get_leads_for_outreach,
    insert_outreach,
)
from tools.build_outreach_email import generate_outreach_email
from tools.gmass_send import send_transactional_email
from tools.send_email import run as send_smtp_email

logger = logging.getLogger("pipeline")


def run(limit=LEADS_PER_DAY):
    """
    Send personalized outreach emails to leads that were included in the daily report.

    Args:
        limit: Maximum number of leads to contact

    Returns:
        Count of emails successfully sent
    """
    transport = (OUTREACH_TRANSPORT or "gmass").strip().lower()
    if transport not in {"gmass", "smtp"}:
        logger.warning(f"OUTREACH_TRANSPORT='{transport}' invalido. Usando 'gmass'.")
        transport = "gmass"

    if transport == "gmass" and not GMASS_API_KEY:
        logger.error("GMASS_API_KEY no configurada. Saltando outreach.")
        return 0

    # Get leads ready for outreach (have been sent in report, have email, not yet contacted)
    leads = get_leads_for_outreach(limit=limit)

    if not leads:
        logger.info("No hay leads listos para outreach individual.")
        return 0

    logger.info(f"Enviando outreach personalizado a {len(leads)} leads...")
    sent_count = 0

    for i, lead in enumerate(leads):
        company = lead.get("company_name", "Unknown")
        email_to = lead.get("email")
        lead_id = lead.get("id")

        if not email_to:
            logger.warning(f"  [{i+1}/{len(leads)}] {company}: Sin email, saltando")
            continue

        logger.info(f"  [{i+1}/{len(leads)}] Procesando: {company} ({email_to})")

        # Generate personalized email
        try:
            email_content = generate_outreach_email(lead, email_type='initial')
            if not email_content:
                logger.warning(f"    -> No se pudo generar email para {company}")
                continue

            subject = email_content["subject"]
            html_body = email_content["html_body"]
            plain_body = email_content["body"]

        except Exception as e:
            logger.error(f"    -> Error generando email: {e}")
            continue

        if transport == "smtp":
            # SMTP (Gmail) send: avoids GMass link/open tracking domains.
            try:
                ok = send_smtp_email(
                    recipient=email_to,
                    subject=subject,
                    html_body=html_body,
                    attachment_path=None,
                )
                if ok:
                    insert_outreach(
                        lead_id=lead_id,
                        email_to=email_to,
                        subject=subject,
                        body=plain_body,
                        outreach_type="initial",
                        gmass_id=None,
                    )
                    sent_count += 1
                    logger.info("    -> Enviado (SMTP)")
                else:
                    logger.error("    -> Error SMTP: send_email returned False")
            except Exception as e:
                logger.error(f"    -> Error enviando email (SMTP): {e}")
        else:
            # Send via GMass Transactional API
            try:
                result = send_transactional_email(
                    to_email=email_to,
                    subject=subject,
                    html_body=html_body,
                    track_opens=False,
                    track_clicks=False,
                )

                if result.get("success"):
                    gmass_id = result.get("message_id")
                    insert_outreach(
                        lead_id=lead_id,
                        email_to=email_to,
                        subject=subject,
                        body=plain_body,
                        outreach_type="initial",
                        gmass_id=gmass_id,
                    )
                    sent_count += 1
                    logger.info("    -> Enviado (GMass)")
                else:
                    logger.error(f"    -> Error GMass: {result.get('error')}")

            except Exception as e:
                logger.error(f"    -> Error enviando email (GMass): {e}")

        # Rate limiting - be nice to APIs
        time.sleep(AI_REQUEST_DELAY)

    logger.info(f"Outreach completado: {sent_count}/{len(leads)} emails enviados")
    return sent_count


if __name__ == "__main__":
    from tools.utils import setup_logging
    from tools.db_manager import init_db

    setup_logging()
    init_db()

    count = run(limit=5)
    print(f"Outreach enviado: {count} emails")

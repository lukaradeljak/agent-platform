"""
Stage 7: Automated Followups.
Sends followup emails to leads that haven't replied after X days.
"""

import logging
import time

from tools.config import AI_REQUEST_DELAY, GMASS_API_KEY, FOLLOWUP_DAYS
from tools.db_manager import (
    get_outreach_needing_followup,
    insert_outreach,
    mark_followup_sent,
)
from tools.build_outreach_email import generate_outreach_email
from tools.gmass_send import send_transactional_email

logger = logging.getLogger("pipeline")


def run():
    """
    Send followup emails to leads that haven't replied after FOLLOWUP_DAYS days.

    Returns:
        Count of followups successfully sent
    """
    if not GMASS_API_KEY:
        logger.error("GMASS_API_KEY no configurada. Saltando followups.")
        return 0

    # Get outreach that needs followup
    outreach_list = get_outreach_needing_followup()

    if not outreach_list:
        logger.info(f"No hay leads que necesiten followup (>{FOLLOWUP_DAYS} dias sin respuesta).")
        return 0

    logger.info(f"Enviando followup a {len(outreach_list)} leads...")
    sent_count = 0

    for i, outreach in enumerate(outreach_list):
        outreach_id = outreach.get("id")
        lead_id = outreach.get("lead_id")
        email_to = outreach.get("email_to")
        company = outreach.get("company_name", "Unknown")
        original_subject = outreach.get("email_subject", "")

        logger.info(f"  [{i+1}/{len(outreach_list)}] Followup: {company} ({email_to})")

        # Build lead dict for email generation
        lead_data = {
            "company_name": company,
            "contact_name": outreach.get("contact_name"),
            "ai_summary": outreach.get("ai_summary"),
            "automation_suggestions": outreach.get("automation_suggestions"),
        }

        # Generate followup email
        try:
            email_content = generate_outreach_email(lead_data, email_type='followup')
            if not email_content:
                logger.warning(f"    -> No se pudo generar followup para {company}")
                continue

            # Use Re: prefix to thread with original email
            subject = f"Re: {original_subject}" if original_subject else email_content["subject"]
            html_body = email_content["html_body"]
            plain_body = email_content["body"]

            logger.debug(f"    -> Subject: {subject}")

        except Exception as e:
            logger.error(f"    -> Error generando followup: {e}")
            continue

        # Send via GMass
        try:
            result = send_transactional_email(
                to_email=email_to,
                subject=subject,
                html_body=html_body,
            )

            if result.get("success"):
                # Record followup in database
                gmass_id = result.get("message_id")
                followup_id = insert_outreach(
                    lead_id=lead_id,
                    email_to=email_to,
                    subject=subject,
                    body=plain_body,
                    outreach_type='followup',
                    gmass_id=gmass_id,
                )

                # Mark original outreach as having followup sent
                mark_followup_sent(outreach_id, followup_id)

                sent_count += 1
                logger.info(f"    -> Followup enviado exitosamente")
            else:
                logger.error(f"    -> Error GMass: {result.get('error')}")

        except Exception as e:
            logger.error(f"    -> Error enviando followup: {e}")

        # Rate limiting
        time.sleep(AI_REQUEST_DELAY)

    logger.info(f"Followups completados: {sent_count}/{len(outreach_list)} enviados")
    return sent_count


if __name__ == "__main__":
    from tools.utils import setup_logging
    from tools.db_manager import init_db

    setup_logging()
    init_db()

    count = run()
    print(f"Followups enviados: {count}")

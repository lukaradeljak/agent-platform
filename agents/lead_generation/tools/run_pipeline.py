"""
Main Pipeline Orchestrator.
Runs all stages in sequence: Discovery -> Enrichment -> AI Analysis -> Excel -> Email -> Outreach -> Followups.
Handles errors gracefully and logs the full run.
"""

import sys
import time
import logging
from datetime import date
from pathlib import Path

# Add project root to path so tools can import each other
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tools.config import LEADS_PER_DAY, RECIPIENT_EMAIL, GMAIL_ADDRESS, GMASS_API_KEY, DB_BACKEND
from tools.utils import setup_logging
from tools import db_manager
from tools import discover_leads
from tools import enrich_leads
from tools import apollo_enrich
from tools import ai_analyze
from tools import build_excel
from tools import build_email_body
from tools import send_email
from tools import send_outreach


def main():
    """Execute the full lead generation pipeline."""
    start_time = time.time()
    run_date = date.today().isoformat()
    logger = setup_logging()
    stats = {}
    errors = []

    logger.info("=" * 60)
    logger.info(f"PIPELINE START | {run_date}")
    logger.info("=" * 60)

    try:
        # --- Stage 0: Initialize Database ---
        logger.info("Stage 0: Initializing database...")
        db_manager.init_db()
        logger.info(f"  DB backend: {DB_BACKEND}")
        logger.info(f"  Total leads in DB: {db_manager.get_total_leads_count()}")
        logger.info(f"  Unsent enriched leads: {db_manager.get_unsent_count()}")

        # --- Stage 1: Discover Leads ---
        logger.info("-" * 40)
        logger.info("Stage 1: Discovering leads via Apollo.io (READ-ONLY)...")
        try:
            discovered = discover_leads.run()
            stats["discovered"] = discovered
            logger.info(f"  -> {discovered} new leads discovered")
        except Exception as e:
            logger.error(f"  -> Discovery FAILED: {e}")
            stats["discovered"] = 0
            errors.append(f"Discovery: {str(e)}")

        # --- Stage 2a: Enrich via Website Scraping ---
        # Scraping gets page content for AI + finds emails for leads without one
        logger.info("-" * 40)
        logger.info("Stage 2a: Enriching leads via website scraping...")
        try:
            enriched = enrich_leads.run()
            stats["enriched"] = enriched
            logger.info(f"  -> {enriched} leads enriched from websites")
        except Exception as e:
            logger.error(f"  -> Website enrichment FAILED: {e}")
            stats["enriched"] = 0
            errors.append(f"Website enrichment: {str(e)}")

        # --- Stage 2b: Apollo Email Enrichment (READ-ONLY) ---
        # For any remaining leads without email, try /people/match with scraped names
        logger.info("-" * 40)
        logger.info("Stage 2b: Email enrichment via Apollo.io (READ-ONLY)...")
        try:
            email_found = apollo_enrich.run()
            stats["with_email"] = email_found
            logger.info(f"  -> {email_found} additional emails found via Apollo")
        except Exception as e:
            logger.error(f"  -> Apollo email enrichment FAILED: {e}")
            stats["with_email"] = 0
            errors.append(f"Apollo enrichment: {str(e)}")

        # --- Stage 3: AI Analysis ---
        logger.info("-" * 40)
        logger.info("Stage 3: AI company analysis (Gemini/OpenAI)...")
        try:
            analyzed = ai_analyze.run()
            stats["ai_analyzed"] = analyzed
            logger.info(f"  -> {analyzed} leads analyzed with AI")
        except Exception as e:
            logger.error(f"  -> AI analysis FAILED: {e}")
            stats["ai_analyzed"] = 0
            errors.append(f"AI analysis: {str(e)}")

        # --- Stage 4: Build Outputs ---
        logger.info("-" * 40)
        logger.info("Stage 4: Building outputs (Excel + email)...")

        leads = db_manager.get_unsent_leads(limit=LEADS_PER_DAY)

        if not leads:
            logger.warning("No enriched leads available to send. Pipeline ending early.")
            logger.warning("This may happen on the first run. Leads need discovery + enrichment + AI analysis.")
            stats["sent"] = 0
        else:
            leads_with_email = sum(1 for lead in leads if lead.get("email"))
            leads_with_phone = sum(1 for lead in leads if lead.get("phone"))
            logger.info(
                f"  Preparing {len(leads)} leads for delivery "
                f"({leads_with_email} con email, {leads_with_phone} con telefono)..."
            )

            # Build Excel
            try:
                excel_path = build_excel.run(leads, run_date)
                logger.info(f"  -> Excel: {excel_path}")
            except Exception as e:
                logger.error(f"  -> Excel build FAILED: {e}")
                excel_path = None
                errors.append(f"Excel: {str(e)}")

            # Build email body
            try:
                email_html = build_email_body.run(leads, run_date)
                logger.info(f"  -> Email body generated ({len(email_html)} chars)")
            except Exception as e:
                logger.error(f"  -> Email body build FAILED: {e}")
                email_html = f"<p>Error generating email body. Check the attached Excel for leads.</p>"
                errors.append(f"Email body: {str(e)}")

            # --- Stage 5: Send Email ---
            logger.info("-" * 40)
            logger.info("Stage 5: Sending email...")

            recipient = RECIPIENT_EMAIL or GMAIL_ADDRESS
            subject = f"Informe Diario de Leads - {run_date} - {len(leads)} Agencias de Marketing"

            try:
                success = send_email.run(
                    recipient=recipient,
                    subject=subject,
                    html_body=email_html,
                    attachment_path=excel_path,
                )
                if success:
                    # Mark leads as sent
                    lead_ids = [lead["id"] for lead in leads]
                    db_manager.mark_leads_sent(lead_ids, run_date)
                    stats["sent"] = len(leads)
                    logger.info(f"  -> Successfully sent {len(leads)} leads to {recipient}")
                else:
                    stats["sent"] = 0
                    errors.append("Email: send_email returned False")
            except Exception as e:
                logger.error(f"  -> Email send FAILED: {e}")
                stats["sent"] = 0
                errors.append(f"Email send: {str(e)}")

        # --- Stage 6: Personalized Outreach via GMass ---
        if GMASS_API_KEY:
            logger.info("-" * 40)
            logger.info("Stage 6: Outreach personalizado via GMass...")
            try:
                outreach_sent = send_outreach.run(limit=LEADS_PER_DAY)
                stats["outreach_sent"] = outreach_sent
                logger.info(f"  -> {outreach_sent} emails de outreach enviados")
            except Exception as e:
                logger.error(f"  -> Outreach FAILED: {e}")
                stats["outreach_sent"] = 0
                errors.append(f"Outreach: {str(e)}")

        else:
            logger.info("-" * 40)
            logger.info("Stage 6: GMass no configurado, saltando outreach")
            stats["outreach_sent"] = 0

    except Exception as e:
        logger.error(f"PIPELINE CRITICAL ERROR: {e}", exc_info=True)
        errors.append(f"Critical: {str(e)}")

    finally:
        duration = time.time() - start_time
        stats["duration_seconds"] = round(duration, 2)
        stats["errors"] = errors

        # Log the run
        try:
            db_manager.log_pipeline_run(stats)
        except Exception:
            pass

        logger.info("=" * 60)
        logger.info(f"PIPELINE COMPLETE | Duration: {duration:.1f}s")
        logger.info(f"  Discovered: {stats.get('discovered', 0)}")
        logger.info(f"  Website enriched: {stats.get('enriched', 0)}")
        logger.info(f"  Apollo emails: {stats.get('with_email', 0)}")
        logger.info(f"  AI analyzed: {stats.get('ai_analyzed', 0)}")
        logger.info(f"  Report sent: {stats.get('sent', 0)}")
        logger.info(f"  Outreach emails: {stats.get('outreach_sent', 0)}")
        if errors:
            logger.warning(f"  Errors: {len(errors)}")
            for err in errors:
                logger.warning(f"    - {err}")
        logger.info("=" * 60)

    return stats


if __name__ == "__main__":
    main()

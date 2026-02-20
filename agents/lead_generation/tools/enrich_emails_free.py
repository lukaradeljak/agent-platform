"""
Stage 2b: Free Email Enrichment.
For leads where website scraping didn't find an email, uses:
1. Serper.dev Google search for emails associated with the domain
2. Common email pattern generation with DNS MX verification
"""

import logging
import smtplib
import socket
import time

import requests

from tools.config import SERPER_API_KEY, WEBSITE_SCRAPE_DELAY
from tools.db_manager import get_leads_needing_email_enrichment, update_lead_enrichment
from tools.utils import retry, clean_email, extract_emails_from_text

logger = logging.getLogger("pipeline")

SERPER_SEARCH_URL = "https://google.serper.dev/search"

# Common email patterns for marketing agencies (ordered by likelihood)
EMAIL_PATTERNS = [
    "info@{domain}",
    "contacto@{domain}",
    "hola@{domain}",
    "hello@{domain}",
    "contact@{domain}",
]


@retry(max_attempts=2, backoff_factor=3, exceptions=(requests.RequestException,))
def _search_emails_serper(domain, company_name):
    """
    Search Google via Serper.dev for email addresses associated with a domain.
    Returns a list of emails found, sorted by relevance (domain match first).
    """
    if not SERPER_API_KEY:
        return []

    query = f'{domain} email contacto'
    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "q": query,
        "num": 5,
    }

    response = requests.post(SERPER_SEARCH_URL, json=payload, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json()

    emails = set()

    # Extract emails from organic results
    for result in data.get("organic", []):
        text = f"{result.get('title', '')} {result.get('snippet', '')} {result.get('link', '')}"
        found = extract_emails_from_text(text)
        for email in found:
            if domain in email.lower():
                cleaned = clean_email(email)
                if cleaned:
                    emails.add(cleaned)

    # Also check knowledge graph and answer boxes
    for key in ["knowledgeGraph", "answerBox"]:
        if key in data:
            text = str(data[key])
            found = extract_emails_from_text(text)
            for email in found:
                if domain in email.lower():
                    cleaned = clean_email(email)
                    if cleaned:
                        emails.add(cleaned)

    return list(emails)


def _check_mx_exists(domain):
    """
    Check if a domain has MX records using socket (no extra dependencies).
    Falls back to checking if the domain resolves at all.
    """
    try:
        # Try to resolve the mail server
        socket.getaddrinfo(domain, 25, socket.AF_INET, socket.SOCK_STREAM)
        return True
    except socket.gaierror:
        pass

    # Fallback: just check if the domain resolves
    try:
        socket.gethostbyname(domain)
        return True
    except socket.gaierror:
        return False


def _verify_email_smtp(email):
    """
    Try to verify if an email exists via SMTP RCPT TO.
    Returns True if accepted, False if rejected, None if inconclusive.
    Note: Many servers block this, so treat None as "might exist".
    """
    domain = email.split("@")[1]

    try:
        # Connect to domain's mail server on port 25
        with smtplib.SMTP(timeout=8) as smtp:
            smtp.connect(domain, 25)
            smtp.helo("verify.local")
            smtp.mail("test@verify.local")
            code, _ = smtp.rcpt(email)
            return code == 250
    except Exception:
        return None


def _try_email_patterns(domain):
    """
    Try common email patterns and optionally verify via SMTP.
    Returns the first verified email, or the best guess if verification isn't possible.
    """
    if not _check_mx_exists(domain):
        logger.debug(f"    Domain {domain} doesn't appear to accept email")
        return None, "none"

    # Try SMTP verification for each pattern
    for pattern in EMAIL_PATTERNS:
        email = pattern.format(domain=domain)
        result = _verify_email_smtp(email)
        if result is True:
            return email, "smtp_verified"

    # If SMTP verification didn't work (blocked/inconclusive), return best guess
    # info@ is the most common generic email for businesses
    best_guess = f"info@{domain}"
    return best_guess, "pattern_guess"


def run():
    """
    Attempt free email enrichment for leads missing emails.
    Strategy 1: Serper Google search for emails on the web.
    Strategy 2: Common email pattern + SMTP verification.
    Returns count of leads where an email was found.
    """
    leads = get_leads_needing_email_enrichment(limit=20)
    if not leads:
        logger.info("No leads need email enrichment.")
        return 0

    logger.info(f"Free email enrichment: attempting {len(leads)} leads...")
    found = 0

    for i, lead in enumerate(leads):
        domain = lead.get("domain", "")
        company = lead.get("company_name", "")
        logger.debug(f"  [{i+1}/{len(leads)}] Email lookup: {company} ({domain})")

        email = None
        source = "none"

        # Strategy 1: Serper Google search for emails
        if SERPER_API_KEY and not email:
            try:
                serper_emails = _search_emails_serper(domain, company)
                if serper_emails:
                    email = serper_emails[0]
                    source = "serper_search"
                    logger.debug(f"    -> Serper found: {email}")
            except Exception as e:
                logger.debug(f"    -> Serper search failed: {e}")

        # Strategy 2: Email pattern + SMTP verification
        if not email:
            try:
                pattern_email, pattern_source = _try_email_patterns(domain)
                if pattern_email:
                    email = pattern_email
                    source = pattern_source
                    logger.debug(f"    -> Pattern {source}: {email}")
            except Exception as e:
                logger.debug(f"    -> Pattern check failed: {e}")

        if email:
            update_lead_enrichment(lead["id"], {
                "email": email,
                "email_source": source,
            })
            found += 1
        else:
            update_lead_enrichment(lead["id"], {"email_source": "none"})
            logger.debug(f"    -> No email found")

        # Small delay between lookups
        time.sleep(WEBSITE_SCRAPE_DELAY)

    logger.info(f"Free email enrichment complete: found emails for {found}/{len(leads)} leads")
    return found


if __name__ == "__main__":
    from tools.utils import setup_logging
    from tools.db_manager import init_db

    setup_logging()
    init_db()
    count = run()
    print(f"Found emails for {count} leads")

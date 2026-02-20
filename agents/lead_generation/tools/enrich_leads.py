"""
Stage 2a: Lead Enrichment via Website Scraping.
Visits each lead's website to extract emails, contact names, and page content for AI analysis.
"""

import logging
import re
import time

import requests
from bs4 import BeautifulSoup

from tools.config import (
    CONTACT_PAGES,
    LOW_PRIORITY_EMAIL_PREFIXES,
    LEADS_PER_DAY,
    WEBSITE_SCRAPE_TIMEOUT,
    WEBSITE_SCRAPE_DELAY,
)
from tools.db_manager import get_leads_needing_enrichment, update_lead_enrichment
from tools.utils import extract_emails_from_text, clean_email, sanitize_text

logger = logging.getLogger("pipeline")

# Common User-Agent to avoid blocks
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


def _fetch_page(url, timeout=WEBSITE_SCRAPE_TIMEOUT):
    """Fetch a web page and return the HTML content."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if response.status_code == 200:
            return response.text
    except requests.RequestException:
        pass
    return None


def _normalize_url(base_url, path):
    """Build a full URL from base and path."""
    base = base_url.rstrip("/")
    if not base.startswith(("http://", "https://")):
        base = "https://" + base
    if path:
        return base + "/" + path.lstrip("/")
    return base


def _extract_phone(html_text):
    """Try to extract a phone number from page text."""
    if not html_text:
        return None
    # Match common phone formats (international)
    patterns = [
        r'\+\d{1,3}[\s.-]?\(?\d{1,4}\)?[\s.-]?\d{3,4}[\s.-]?\d{3,4}',  # +34 91 123 4567
        r'\(\d{2,4}\)\s?\d{3,4}[\s.-]?\d{3,4}',  # (91) 123 4567
        r'\b\d{2,4}[\s.-]\d{2,4}[\s.-]\d{2,4}(?:[\s.-]\d{2,4})?\b',  # 91 123 45 67 / 600-123-456
        r'\+\d{1,3}\s?\d{6,12}\b',  # +34911234567
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text)
        if match:
            phone = match.group().strip()
            # Basic validation: at least 8 digits
            digits = re.sub(r'\D', '', phone)
            if 8 <= len(digits) <= 14:
                return phone
    return None


def _extract_contact_name(soup):
    """Try to extract a contact person's name from the page."""
    # Look for common patterns near role titles
    role_keywords = [
        "CEO", "Fundador", "Founder", "Director", "Managing Director",
        "Directora", "Cofundador", "Co-founder", "Owner", "Gerente",
        "Socio", "Partner",
    ]

    text = soup.get_text(separator=" ")
    for keyword in role_keywords:
        # Look for "Name - Role" or "Name, Role" patterns
        patterns = [
            rf'([A-Z][a-záéíóúñ]+ [A-Z][a-záéíóúñ]+(?:\s[A-Z][a-záéíóúñ]+)?)\s*[,\-–|]\s*{keyword}',
            rf'{keyword}\s*[,\-–|:]\s*([A-Z][a-záéíóúñ]+ [A-Z][a-záéíóúñ]+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                # Basic validation: 2-4 words, reasonable length
                words = name.split()
                if 2 <= len(words) <= 4 and len(name) < 60:
                    return name

    return None


def _prioritize_emails(emails):
    """Sort emails with personal ones first, generic ones last."""
    personal = []
    generic_good = []  # info@, hello@, hola@, contacto@
    generic_bad = []   # noreply, support, etc.

    for email in emails:
        prefix = email.split("@")[0].lower()
        if any(prefix.startswith(lp) for lp in LOW_PRIORITY_EMAIL_PREFIXES):
            generic_bad.append(email)
        elif prefix in ("info", "hello", "hola", "contacto", "contact", "ventas", "sales"):
            generic_good.append(email)
        else:
            personal.append(email)

    return personal + generic_good + generic_bad


def _scrape_lead(lead):
    """
    Scrape a single lead's website for contact info and page content.
    Returns a dict with extracted data.
    """
    website = lead.get("website", "")
    if not website:
        return {}

    all_emails = []
    all_text = []
    phone = None
    contact_name = None

    for page_path in CONTACT_PAGES:
        url = _normalize_url(website, page_path)
        html = _fetch_page(url)

        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # Remove script and style elements for cleaner text
        for tag in soup(["script", "style", "noscript", "iframe"]):
            tag.decompose()

        page_text = soup.get_text(separator=" ")

        # Extract emails
        emails = extract_emails_from_text(page_text)
        # Also check href="mailto:" links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.startswith("mailto:"):
                email = clean_email(href.replace("mailto:", "").split("?")[0])
                if email:
                    emails.append(email)

        all_emails.extend(emails)

        # Extract phone (only if we don't have one yet)
        if not phone:
            # Check tel: links first
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if href.startswith("tel:"):
                    phone = href.replace("tel:", "").strip()
                    break
            if not phone:
                phone = _extract_phone(page_text)

        # Extract contact name
        if not contact_name:
            contact_name = _extract_contact_name(soup)

        # Collect page text for AI analysis
        if page_path in ("", "/nosotros", "/about", "/about-us", "/sobre-nosotros"):
            clean_text = sanitize_text(page_text, max_length=800)
            if clean_text and len(clean_text) > 50:
                all_text.append(clean_text)

        time.sleep(0.5)  # Small delay between pages of same site

    # Deduplicate and prioritize emails
    unique_emails = list(dict.fromkeys(all_emails))  # preserve order, remove dupes
    prioritized = _prioritize_emails(unique_emails)
    best_email = prioritized[0] if prioritized else None
    email_source = "website_scrape" if best_email else None

    # Combine scraped text
    scraped_text = " | ".join(all_text) if all_text else None

    result = {}
    if best_email:
        result["email"] = best_email
        result["email_source"] = email_source
    if contact_name:
        result["contact_name"] = contact_name
    if phone:
        result["phone"] = phone
    if scraped_text:
        result["scraped_text"] = scraped_text

    return result


def run():
    """
    Enrich all leads that need website scraping.
    Returns count of leads that got at least some enrichment.
    """
    leads = get_leads_needing_enrichment(limit=LEADS_PER_DAY)
    if not leads:
        logger.info("No leads need website enrichment.")
        return 0

    logger.info(f"Enriching {len(leads)} leads via website scraping...")
    enriched = 0

    for i, lead in enumerate(leads):
        company = lead["company_name"]
        logger.debug(f"  [{i+1}/{len(leads)}] Scraping: {company} ({lead.get('website', 'N/A')})")

        try:
            data = _scrape_lead(lead)
            if data:
                update_lead_enrichment(lead["id"], data)
                enriched += 1
                if data.get("email"):
                    logger.debug(f"    -> Found email: {data['email']}")
                if data.get("contact_name"):
                    logger.debug(f"    -> Found contact: {data['contact_name']}")
        except Exception as e:
            logger.warning(f"    -> Scrape failed for {company}: {e}")

        # Delay between different sites
        time.sleep(WEBSITE_SCRAPE_DELAY)

    logger.info(f"Website enrichment complete: {enriched}/{len(leads)} leads enriched")
    return enriched


if __name__ == "__main__":
    from tools.utils import setup_logging
    from tools.db_manager import init_db

    setup_logging()
    init_db()
    count = run()
    print(f"Enriched {count} leads via website scraping")

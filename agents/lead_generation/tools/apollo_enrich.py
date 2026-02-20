"""
Apollo.io Enrichment Module (READ-ONLY)

Enriches leads with email addresses and additional contact info from Apollo.
CRITICAL: This module only READS data - it NEVER creates contacts, lists, or any other data in Apollo.
"""

import logging
import time

import requests

from tools.config import (
    APOLLO_API_KEY,
    APOLLO_API_BASE,
    APOLLO_RATE_LIMIT_DELAY,
)
from tools.db_manager import (
    get_leads_needing_email_enrichment,
    get_leads_missing_phone,
    update_lead_enrichment,
)
from tools.utils import retry

logger = logging.getLogger("pipeline")


@retry(max_attempts=3, backoff_factor=2, exceptions=(requests.RequestException,))
def _apollo_request(endpoint: str, payload: dict, method: str = "POST") -> dict:
    """
    Make a READ-ONLY request to Apollo API.

    IMPORTANT: Only use with enrichment/match endpoints, NEVER with create endpoints.
    """
    url = f"{APOLLO_API_BASE}/{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": APOLLO_API_KEY,
    }

    if method == "GET":
        response = requests.get(url, params=payload, headers=headers, timeout=30)
    else:
        response = requests.post(url, json=payload, headers=headers, timeout=30)

    response.raise_for_status()
    return response.json()


def enrich_person(domain: str, name: str = None, linkedin_url: str = None) -> dict:
    """
    Enrich a single person to get their email address.

    Uses POST /people/match - READ-ONLY enrichment endpoint.
    Does NOT create any contacts in Apollo.

    Args:
        domain: Company domain (required)
        name: Person's full name (optional but recommended)
        linkedin_url: Person's LinkedIn URL (optional)

    Returns:
        Dict with: email, phone, title, email_source, or empty dict if not found
    """
    if not APOLLO_API_KEY:
        logger.error("APOLLO_API_KEY not configured")
        return {}

    if not domain:
        return {}

    # Build the match payload
    payload = {
        "organization_domain": domain,
    }

    # Add name if provided
    if name:
        parts = name.split(maxsplit=1)
        if len(parts) >= 1:
            payload["first_name"] = parts[0]
        if len(parts) >= 2:
            payload["last_name"] = parts[1]

    # Add LinkedIn if provided (strong identifier)
    if linkedin_url:
        payload["linkedin_url"] = linkedin_url

    try:
        data = _apollo_request("people/match", payload)
        person = data.get("person", {})

        if not person:
            logger.debug(f"No match found for {domain} / {name}")
            return {}

        result = {
            "email": person.get("email"),
            "phone": person.get("phone_numbers", [{}])[0].get("raw_number") if person.get("phone_numbers") else None,
            "title": person.get("title"),
            "contact_name": f"{person.get('first_name', '')} {person.get('last_name', '')}".strip() or None,
            "email_source": "apollo" if person.get("email") else None,
            "linkedin_url": person.get("linkedin_url"),
        }

        # Clean up None values
        return {k: v for k, v in result.items() if v is not None}

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            logger.debug(f"No Apollo match for {domain}")
        else:
            logger.warning(f"Apollo enrichment failed for {domain}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error enriching {domain}: {e}")
        return {}


def enrich_organization(domain: str) -> dict:
    """
    Enrich organization data by domain.

    Uses GET /organizations/enrich - READ-ONLY endpoint.
    Does NOT create any accounts in Apollo.

    Args:
        domain: Company domain

    Returns:
        Dict with: employee_count, industry, description, or empty dict if not found
    """
    if not APOLLO_API_KEY or not domain:
        return {}

    try:
        data = _apollo_request("organizations/enrich", {"domain": domain}, method="GET")
        org = data.get("organization", {})

        if not org:
            return {}

        return {
            "employee_count": org.get("estimated_num_employees"),
            "industry": org.get("industry"),
            "description": org.get("short_description"),
        }

    except requests.HTTPError as e:
        logger.debug(f"Organization enrichment failed for {domain}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error enriching org {domain}: {e}")
        return {}


def enrich_organization_phone(domain: str) -> str | None:
    """
    Fetch organization phone by domain (READ-ONLY).
    Returns phone string or None.
    """
    if not APOLLO_API_KEY or not domain:
        return None

    try:
        data = _apollo_request("organizations/enrich", {"domain": domain}, method="GET")
        org = data.get("organization", {})
        phone = org.get("phone")
        return phone.strip() if isinstance(phone, str) and phone.strip() else None
    except requests.HTTPError:
        return None
    except Exception as e:
        logger.debug(f"Phone enrichment failed for {domain}: {e}")
        return None


def run() -> int:
    """
    Main enrichment function - enriches all leads that need email addresses.

    Returns the count of successfully enriched leads.
    """
    if not APOLLO_API_KEY:
        logger.error("APOLLO_API_KEY not configured. Skipping Apollo enrichment.")
        return 0

    # Get leads that need email enrichment
    leads = get_leads_needing_email_enrichment()
    if not leads:
        logger.info("No leads need email enrichment")
        return 0

    logger.info(f"Enriching {len(leads)} leads via Apollo...")
    enriched_count = 0

    for lead in leads:
        lead_id = lead.get("id")
        domain = lead.get("domain")
        contact_name = lead.get("contact_name")
        linkedin_url = lead.get("linkedin_url")

        logger.debug(f"Enriching: {domain} / {contact_name}")

        # Try to enrich the person
        enrichment_data = enrich_person(
            domain=domain,
            name=contact_name,
            linkedin_url=linkedin_url
        )

        if enrichment_data:
            # Update the lead in database
            # Only mark email_source='apollo' when we actually got an email.
            # Otherwise keep it retryable (some matches return name/title/phone but no email).
            email_value = enrichment_data.get("email")
            update_data = {
                "email": email_value,
                "contact_name": enrichment_data.get("contact_name") or contact_name,
                "phone": enrichment_data.get("phone"),
            }
            if email_value:
                update_data["email_source"] = "apollo"
            else:
                # Track that enrichment was attempted without excluding it from future retries.
                update_data["email_source"] = "none"
            update_lead_enrichment(lead_id, update_data)

            if email_value:
                enriched_count += 1
                logger.debug(f"  -> Found email: {email_value}")
            else:
                logger.debug(f"  -> No email found")
        else:
            logger.debug(f"  -> No enrichment data")

        # Rate limiting - Apollo has limits
        time.sleep(APOLLO_RATE_LIMIT_DELAY)

    logger.info(f"Apollo email enrichment complete: {enriched_count}/{len(leads)} emails found")

    # Second pass: fill missing phone numbers by domain
    phone_leads = get_leads_missing_phone()
    phone_updated = 0

    if phone_leads:
        logger.info(f"Enriching phones via Apollo organizations for {len(phone_leads)} leads...")
        for lead in phone_leads:
            lead_id = lead.get("id")
            domain = lead.get("domain")
            if not domain:
                continue

            phone = enrich_organization_phone(domain)
            if phone:
                update_lead_enrichment(lead_id, {"phone": phone})
                phone_updated += 1

            time.sleep(APOLLO_RATE_LIMIT_DELAY)

    logger.info(f"Apollo phone enrichment complete: {phone_updated}/{len(phone_leads)} phones found")
    return enriched_count


if __name__ == "__main__":
    # Test the module
    from tools.utils import setup_logging
    from tools.db_manager import init_db

    setup_logging()
    init_db()

    count = run()
    print(f"Enriched {count} leads")

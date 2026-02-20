"""
Apollo.io Search Module (READ-ONLY)

Searches Apollo's database for marketing agencies by location.
CRITICAL: This module only READS data - it NEVER creates contacts, lists, or any other data in Apollo.
"""

import logging
import time
import os

import requests

from tools.config import (
    APOLLO_API_KEY,
    APOLLO_API_BASE,
    APOLLO_RATE_LIMIT_DELAY,
    APOLLO_INDUSTRY_KEYWORDS,
    APOLLO_TARGET_TITLES,
    APOLLO_RESULTS_PER_PAGE,
    EXCLUDED_DOMAINS,
)
from tools.utils import extract_domain, is_excluded_domain, retry

logger = logging.getLogger("pipeline")


@retry(max_attempts=3, backoff_factor=2, exceptions=(requests.RequestException,))
def _apollo_request(endpoint: str, payload: dict) -> dict:
    """
    Make a READ-ONLY request to Apollo API.

    IMPORTANT: This function should ONLY be called with search/read endpoints.
    NEVER use this with endpoints that create/modify data.
    """
    url = f"{APOLLO_API_BASE}/{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": APOLLO_API_KEY,
    }

    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def search_companies_by_location(city: str, country: str, limit: int = 50) -> list[dict]:
    """
    Search Apollo for marketing/advertising companies in a specific location.

    Uses POST /mixed_companies/search - READ-ONLY endpoint.
    Does NOT create any data in Apollo.

    Args:
        city: City name to search
        country: Country name
        limit: Maximum number of results

    Returns:
        List of company dicts with: domain, company_name, website, city, country, employee_count
    """
    if not APOLLO_API_KEY:
        logger.error("APOLLO_API_KEY not configured")
        return []

    # Build location query - Apollo uses specific location format
    location_query = f"{city}, {country}"

    payload = {
        "organization_locations": [location_query],
        "q_organization_keyword_tags": APOLLO_INDUSTRY_KEYWORDS,
        "per_page": min(limit, APOLLO_RESULTS_PER_PAGE),
        "page": 1,
    }

    all_companies = []
    seen_domains = set()
    pages_to_fetch = (limit // APOLLO_RESULTS_PER_PAGE) + 1

    for page in range(1, pages_to_fetch + 1):
        payload["page"] = page

        try:
            logger.debug(f"Apollo company search page {page}: {location_query}")
            data = _apollo_request("mixed_companies/search", payload)

            organizations = data.get("organizations", [])
            if not organizations:
                break

            for org in organizations:
                domain = org.get("primary_domain") or org.get("website_url", "")
                if domain:
                    domain = extract_domain(domain)

                if not domain or domain in seen_domains:
                    continue

                # Skip excluded domains
                if is_excluded_domain(f"https://{domain}", EXCLUDED_DOMAINS):
                    continue

                seen_domains.add(domain)
                all_companies.append({
                    "domain": domain,
                    "company_name": org.get("name", "Unknown Agency"),
                    "website": f"https://{domain}",
                    "city": city,
                    "country": country,
                    "snippet": org.get("short_description", ""),
                    "phone": org.get("phone"),  # Apollo provides company phone
                    "employee_count": org.get("estimated_num_employees"),
                    "apollo_org_id": org.get("id"),
                })

                if len(all_companies) >= limit:
                    break

            if len(all_companies) >= limit:
                break

            # Rate limiting
            time.sleep(APOLLO_RATE_LIMIT_DELAY)

        except requests.HTTPError as e:
            logger.warning(f"Apollo company search failed (page {page}): {e}")
            break
        except Exception as e:
            logger.error(f"Unexpected error in Apollo company search: {e}")
            break

    logger.info(f"Apollo found {len(all_companies)} companies in {city}, {country}")
    return all_companies


def _bulk_enrich_people(person_ids: list[str]) -> list[dict]:
    """
    Enrich people by their Apollo IDs using /people/bulk_match.
    Returns full profiles with emails, names, and organization domains.
    Max 10 per request.
    """
    if not person_ids:
        return []

    details = [{"id": pid} for pid in person_ids]
    try:
        data = _apollo_request("people/bulk_match", {"details": details})
        return data.get("matches", [])
    except requests.HTTPError as e:
        logger.warning(f"Bulk enrichment failed: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error in bulk enrichment: {e}")
        return []


def search_people_at_companies(city: str, country: str, limit: int = 50) -> list[dict]:
    """
    Search Apollo for decision-makers at marketing agencies, then bulk-enrich
    to get their emails and full organization data.

    Step 1: POST /mixed_people/api_search → person IDs + first names
    Step 2: POST /people/bulk_match with IDs → emails + domains + full names

    Does NOT create any contacts in Apollo.
    """
    if not APOLLO_API_KEY:
        logger.error("APOLLO_API_KEY not configured")
        return []

    location_query = f"{city}, {country}"

    payload = {
        "person_titles": APOLLO_TARGET_TITLES,
        "organization_locations": [location_query],
        "q_organization_keyword_tags": APOLLO_INDUSTRY_KEYWORDS,
        "per_page": APOLLO_RESULTS_PER_PAGE,
        "page": 1,
    }

    # Step 1: Collect person IDs + org names from search
    person_ids = []
    search_data = {}  # id -> {org_name, first_name, title}

    # Apollo results often contain multiple people from the same organization.
    # If we only fetch `limit` people IDs, deduping by org domain can produce
    # fewer than `limit` unique leads (e.g., always ~26/30). Oversample IDs.
    oversample_raw = os.getenv("APOLLO_PEOPLE_OVERSAMPLE_FACTOR", "3").strip()
    try:
        oversample_factor = int(oversample_raw)
    except ValueError:
        oversample_factor = 3
    oversample_factor = max(1, min(oversample_factor, 10))
    target_person_ids = max(limit, limit * oversample_factor)

    pages_to_fetch = (target_person_ids // APOLLO_RESULTS_PER_PAGE) + 1

    for page in range(1, pages_to_fetch + 1):
        payload["page"] = page

        try:
            logger.debug(f"Apollo people search page {page}: {location_query}")
            data = _apollo_request("mixed_people/api_search", payload)

            people = data.get("people", [])
            if not people:
                break

            for person in people:
                pid = person.get("id")
                if not pid:
                    continue

                person_ids.append(pid)
                search_data[pid] = {
                    "org_name": person.get("organization", {}).get("name", ""),
                    "first_name": person.get("first_name", ""),
                    "title": person.get("title", ""),
                }

                if len(person_ids) >= target_person_ids:
                    break

            if len(person_ids) >= target_person_ids:
                break

            time.sleep(APOLLO_RATE_LIMIT_DELAY)

        except requests.HTTPError as e:
            logger.warning(f"Apollo people search failed (page {page}): {e}")
            break
        except Exception as e:
            logger.error(f"Unexpected error in Apollo people search: {e}")
            break

    if not person_ids:
        logger.info(f"Apollo people search found 0 people in {city}, {country}")
        return []

    logger.info(f"Apollo people search found {len(person_ids)} people in {city}, {country}, enriching...")

    # Step 2: Bulk enrich in batches of 10
    all_leads = []
    seen_domains = set()

    for i in range(0, len(person_ids), 10):
        batch = person_ids[i:i + 10]
        matches = _bulk_enrich_people(batch)

        for match in matches:
            org = match.get("organization", {})
            domain = org.get("primary_domain") or ""
            if domain:
                domain = extract_domain(domain)

            if not domain:
                # Try website_url as fallback
                website = org.get("website_url", "")
                if website:
                    domain = extract_domain(website)

            if not domain or domain in seen_domains:
                continue

            if is_excluded_domain(f"https://{domain}", EXCLUDED_DOMAINS):
                continue

            seen_domains.add(domain)

            first_name = match.get("first_name", "")
            last_name = match.get("last_name", "")
            contact_name = f"{first_name} {last_name}".strip() or None
            email = match.get("email")

            all_leads.append({
                "domain": domain,
                "company_name": org.get("name", "Unknown Agency"),
                "website": f"https://{domain}",
                "city": city,
                "country": country,
                "snippet": org.get("short_description", ""),
                "phone": org.get("phone"),
                "contact_name": contact_name,
                "contact_title": match.get("title"),
                "email": email,
                "email_source": "apollo" if email else None,
                "apollo_person_id": match.get("id"),
                "apollo_org_id": org.get("id"),
                "linkedin_url": match.get("linkedin_url"),
            })

            if len(all_leads) >= limit:
                break

        if len(all_leads) >= limit:
            break

        time.sleep(APOLLO_RATE_LIMIT_DELAY)

    emails_found = sum(1 for l in all_leads if l.get("email"))
    logger.info(f"Apollo enriched {len(all_leads)} leads in {city}, {country} ({emails_found} with email)")
    return all_leads


def search_by_location(city: str, country: str, limit: int = 50) -> list[dict]:
    """
    Main search function - searches for leads by location.

    Uses people search as primary method (returns contact names and titles,
    which makes email enrichment via Apollo /people/match far more effective).
    Falls back to company search if people search returns no results.

    Returns list of leads ready for DB insertion.
    """
    # People search first - gives us contact names, titles, and LinkedIn URLs
    # which dramatically improves Apollo email enrichment hit rate
    leads = search_people_at_companies(city, country, limit)

    if leads:
        return leads[:limit]

    # If people search returned nothing, try company search as fallback
    logger.info("People search returned 0 results, trying company search...")
    leads = search_companies_by_location(city, country, limit)

    return leads[:limit]


if __name__ == "__main__":
    # Test the module
    from tools.utils import setup_logging
    setup_logging()

    results = search_by_location("Madrid", "Espana", limit=5)
    for r in results:
        print(f"  {r['company_name']} - {r['domain']}")

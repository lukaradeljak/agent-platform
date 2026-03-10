"""
apollo_search.py
----------------
Searches Apollo.io People API for leads and saves them to .tmp/leads.csv.

Usage:
    python tools/apollo_search.py --country "España" --limit 40
    python tools/apollo_search.py --limit 40  # uses next country in rotation

Env vars required:
    APOLLO_API_KEY
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

COUNTRY_ROTATION = [
    "Spain",
    "Mexico",
    "Argentina",
    "Chile",
    "Colombia",
    "Uruguay",
    "Panama",
    "Ecuador",
    "Peru",
]

# Country display names → Apollo API country codes
COUNTRY_MAP = {
    "España": "Spain",
    "México": "Mexico",
    "Argentina": "Argentina",
    "Chile": "Chile",
    "Colombia": "Colombia",
    "Uruguay": "Uruguay",
    "Panamá": "Panama",
    "Ecuador": "Ecuador",
    "Perú": "Peru",
    "Spain": "Spain",
    "Mexico": "Mexico",
    "Panama": "Panama",
    "Peru": "Peru",
}

TARGET_TITLES = [
    "CEO",
    "COO",
    "CTO",
    "Chief Executive Officer",
    "Chief Operating Officer",
    "Chief Technology Officer",
    "Director General",
    "Director de Operaciones",
    "Gerente General",
    "Founder",
    "Co-Founder",
]

TECH_KEYWORDS = [
    "software", "soft", "informática", "informatica", "tech", "digital",
    "soluciones tecnol", "developer", "consulting it", "it consulting",
    "cloud", "cyber", "saas", "erp", "crm", "hosting",
    "inteligencia artificial", "web agency", "web studio",
    "ai-powered", "powered by ai", "machine learning", "fintech", "proptech",
    "healthtech", "edtech", "insurtech", "legaltech",
]

TITLE_VERIFY_KEYWORDS = [
    "ceo", "coo", "cto", "cfo", "chief", "founder", "co-founder", "cofundador",
    "director general", "director de operaciones", "gerente general",
    "owner", "presidente", "president", "propietario", "dueño",
    "socio director", "socio fundador",
]

ROTATION_STATE_FILE = Path(".tmp/country_rotation.json")
TMP_DIR = Path(".tmp")
OUTPUT_FILE = TMP_DIR / "leads.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_rotation_state() -> dict:
    if ROTATION_STATE_FILE.exists():
        with open(ROTATION_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"index": 0}


def save_rotation_state(state: dict):
    TMP_DIR.mkdir(exist_ok=True)
    with open(ROTATION_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def get_next_country(override: str | None) -> str:
    if override:
        return COUNTRY_MAP.get(override, override)
    state = load_rotation_state()
    idx = state.get("index", 0) % len(COUNTRY_ROTATION)
    country = COUNTRY_ROTATION[idx]
    state["index"] = (idx + 1) % len(COUNTRY_ROTATION)
    save_rotation_state(state)
    return country


def enrich_person(api_key: str, person_id: str, headers: dict) -> dict | None:
    """Call People Enrichment to reveal email for a given Apollo person ID."""
    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.apollo.io/api/v1/people/match",
                headers=headers,
                json={"id": person_id, "reveal_personal_emails": True},
                timeout=30,
            )
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"  Connection reset by Apollo. Retrying in {wait}s...")
            time.sleep(wait)
            continue

        if r.status_code == 429:
            print("  Rate limited on enrichment. Waiting 60s...")
            time.sleep(60)
            continue

        if r.status_code != 200:
            return None

        return r.json().get("person")

    return None


def search_people(api_key: str, country: str, limit: int) -> list[dict]:
    """Call Apollo People Search then enrich each result to get emails."""
    search_url = "https://api.apollo.io/api/v1/mixed_people/api_search"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "x-api-key": api_key,
    }

    leads = []
    seen_emails = set()
    seen_person_ids = set()
    page = 1
    per_page = 25

    while len(leads) < limit:
        payload = {
            "person_titles": TARGET_TITLES,
            "organization_num_employees_ranges": ["1,200"],
            "person_locations": [country],
            "has_email": True,
            "organization_not_industries": [
                "computer software",
                "information technology and services",
                "internet",
                "computer & network security",
                "computer hardware",
                "semiconductors",
                "telecommunications",
                "staffing and recruiting",
            ],
            "page": page,
            "per_page": per_page,
        }

        response = requests.post(search_url, headers=headers, json=payload, timeout=30)

        if response.status_code == 429:
            print("Rate limited by Apollo. Waiting 60s...")
            time.sleep(60)
            continue

        if response.status_code != 200:
            print(f"Apollo API error {response.status_code}: {response.text}")
            break

        data = response.json()
        people = data.get("people", [])

        if not people:
            print(f"No more results from Apollo (page {page}).")
            break

        for person in people:
            if len(leads) >= limit:
                break

            person_id = person.get("id", "")
            if not person_id or person_id in seen_person_ids:
                continue
            seen_person_ids.add(person_id)

            print(f"  Enriching {person.get('first_name', '')} ({len(leads)+1}/{limit})...")
            enriched = enrich_person(api_key, person_id, headers)
            if not enriched:
                continue

            email = (enriched.get("email") or "").lower().strip()
            if not email or "@" not in email or email in seen_emails:
                continue

            actual_title = (enriched.get("title") or "").lower()
            if not any(kw in actual_title for kw in TITLE_VERIFY_KEYWORDS):
                print(f"    Skipping non-target title: {enriched.get('title')}")
                continue

            company_name = ((enriched.get("organization") or {}).get("name") or "").lower()
            if any(kw in company_name for kw in TECH_KEYWORDS):
                print(f"    Skipping tech company: {company_name}")
                continue

            seen_emails.add(email)

            phone_numbers = enriched.get("phone_numbers") or []
            phone = phone_numbers[0].get("sanitized_number", "") if phone_numbers else ""
            if not phone:
                phone = (enriched.get("organization") or {}).get("phone", "") or ""

            leads.append({
                "first_name": enriched.get("first_name", ""),
                "last_name": enriched.get("last_name", ""),
                "email": email,
                "title": enriched.get("title", ""),
                "company": (enriched.get("organization") or {}).get("name", ""),
                "country": country,
                "linkedin_url": enriched.get("linkedin_url", ""),
                "phone": phone,
            })
            time.sleep(1.5)  # polite delay between enrichments

        page += 1
        time.sleep(1)  # polite delay between search pages

    return leads


def save_leads(leads: list[dict]):
    TMP_DIR.mkdir(exist_ok=True)
    fieldnames = ["first_name", "last_name", "email", "title", "company", "country", "linkedin_url", "phone"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(leads)
    print(f"Saved {len(leads)} leads -> {OUTPUT_FILE}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Search Apollo for leads.")
    parser.add_argument("--country", type=str, default=None,
                        help="Country to search (overrides rotation). E.g. 'España'")
    parser.add_argument("--limit", type=int, default=40,
                        help="Max number of leads to retrieve (default: 40)")
    args = parser.parse_args()

    api_key = os.getenv("APOLLO_API_KEY")
    if not api_key:
        print("ERROR: APOLLO_API_KEY not set in .env")
        sys.exit(1)

    country = get_next_country(args.country)
    print(f"Searching Apollo for leads in: {country} (limit: {args.limit})")

    leads = search_people(api_key, country, args.limit)

    if not leads:
        print("No leads found. Check your Apollo filters or API key.")
        sys.exit(1)

    save_leads(leads)
    print(f"Done. Country for next run: {COUNTRY_ROTATION[load_rotation_state()['index'] % len(COUNTRY_ROTATION)]}")


if __name__ == "__main__":
    main()

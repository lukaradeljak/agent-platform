"""
Stage 1: Lead Discovery via Apollo.io API (READ-ONLY).
Searches for marketing agencies in the next rotated city and stores new leads in the DB.

IMPORTANT: This module only READS from Apollo - it never creates contacts, lists, or any data.
"""

import logging

from tools.config import (
    APOLLO_API_KEY,
    LEADS_PER_DAY,
)
from tools.db_manager import (
    get_next_city,
    update_city_searched,
    lead_exists,
    insert_lead,
)
from tools.apollo_search import search_by_location

logger = logging.getLogger("pipeline")


def run():
    """
    Execute lead discovery for the next city in rotation using Apollo.
    Returns the number of new leads inserted.
    """
    if not APOLLO_API_KEY:
        logger.error("APOLLO_API_KEY not configured. Skipping discovery.")
        return 0

    inserted = 0
    attempted_cities = []
    max_city_attempts = 10

    while inserted < LEADS_PER_DAY and len(attempted_cities) < max_city_attempts:
        city_data = get_next_city()
        if not city_data:
            logger.error("No cities in rotation. Check db_manager.init_db().")
            break

        city_name = city_data["city_name"]
        country = city_data["country"]
        city_key = (city_name, country)
        if city_key in attempted_cities:
            break

        remaining = LEADS_PER_DAY - inserted
        search_limit = remaining + 10  # Margin for duplicates/excluded domains

        logger.info(f"Discovering leads in: {city_name}, {country}")
        all_leads = search_by_location(city_name, country, limit=search_limit)

        inserted_this_city = 0
        for lead in all_leads:
            if not lead_exists(lead["domain"]):
                result = insert_lead(lead)
                if result is not None:
                    inserted += 1
                    inserted_this_city += 1
                    if inserted >= LEADS_PER_DAY:
                        break

        # Move city pointer forward even if we got 0 leads, to avoid getting stuck.
        update_city_searched(city_name, country)
        attempted_cities.append(city_key)

        logger.info(
            f"Discovery complete: {city_name}, {country} | "
            f"Found {len(all_leads)} results, {inserted_this_city} new leads inserted "
            f"(total {inserted}/{LEADS_PER_DAY})"
        )

    if inserted < LEADS_PER_DAY:
        logger.warning(
            f"Discovery target not reached after {len(attempted_cities)} cities: "
            f"{inserted}/{LEADS_PER_DAY}"
        )

    logger.info(f"Total new leads inserted: {inserted}")
    return inserted


if __name__ == "__main__":
    from tools.utils import setup_logging
    from tools.db_manager import init_db

    setup_logging()
    init_db()
    count = run()
    print(f"Discovered {count} new leads")

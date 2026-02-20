"""
Centralized configuration for the Lead Generation & Enrichment Pipeline.
Loads all environment variables and defines constants used across tools.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
TMP_DIR = Path(os.environ.get("PIPELINE_TMP_DIR", str(PROJECT_ROOT / ".tmp")))
DB_PATH = TMP_DIR / "leads.db"
LOG_PATH = TMP_DIR / "pipeline.log"

# Ensure .tmp directory exists
TMP_DIR.mkdir(exist_ok=True)

# Load environment variables
load_dotenv(PROJECT_ROOT / ".env")


def _get_int_env(name: str, default: int) -> int:
    """Read an integer env var with safe fallback."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        return default

def _get_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default

# --- Database (SQLite by default, Supabase Postgres optional) ---
# If SUPABASE_DB_URL or DATABASE_URL is set, db_manager will use Postgres.
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "").strip() or os.getenv("DATABASE_URL", "").strip()
DB_BACKEND = "supabase_postgres" if SUPABASE_DB_URL else "sqlite"

# --- API Keys ---
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# --- Apollo.io Configuration (READ-ONLY) ---
APOLLO_API_BASE = "https://api.apollo.io/api/v1"
APOLLO_RATE_LIMIT_DELAY = 1.0  # seconds between requests

# --- Email ---
GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL", "") or GMAIL_ADDRESS  # defaults to same

# --- GMass API (Outreach) ---
GMASS_API_KEY = os.getenv("GMASS_API_KEY", "").strip()
GMASS_API_BASE = "https://api.gmass.co/api"
GMASS_FROM_NAME = "Luka Radeljak"  # Nombre que aparece en los emails de outreach
FOLLOWUP_DAYS = 3  # Dias de espera antes de enviar followup
GMASS_TRACK_OPENS = _get_bool_env("GMASS_TRACK_OPENS", False)
GMASS_TRACK_CLICKS = _get_bool_env("GMASS_TRACK_CLICKS", False)
OUTREACH_TRANSPORT = os.getenv("OUTREACH_TRANSPORT", "gmass").strip().lower()  # gmass|smtp

# --- Pipeline Settings ---
LEADS_PER_DAY = _get_int_env("LEADS_PER_DAY", 30)
MAX_SEARCH_RESULTS_PER_QUERY = 20
WEBSITE_SCRAPE_TIMEOUT = 10  # seconds
WEBSITE_SCRAPE_DELAY = 1.0   # seconds between requests
AI_REQUEST_DELAY = 4.0       # seconds between AI API calls

# --- AI Model Configuration ---
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
OPENAI_MODEL = "gpt-4o-mini"
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
AI_MAX_TOKENS = 1500
AI_TEMPERATURE = 0.7

# --- City Rotation ---
# Each entry: (city, country, language_hint)
# language_hint helps the AI generate content in the right language
CITIES = [
    # Spain
    ("Madrid", "Espana", "es"),
    ("Barcelona", "Espana", "es"),
    ("Valencia", "Espana", "es"),
    ("Sevilla", "Espana", "es"),
    ("Bilbao", "Espana", "es"),
    ("Malaga", "Espana", "es"),
    ("Zaragoza", "Espana", "es"),
    ("Alicante", "Espana", "es"),
    # Mexico
    ("Ciudad de Mexico", "Mexico", "es"),
    ("Guadalajara", "Mexico", "es"),
    ("Monterrey", "Mexico", "es"),
    ("Puebla", "Mexico", "es"),
    ("Queretaro", "Mexico", "es"),
    # Argentina
    ("Buenos Aires", "Argentina", "es"),
    ("Cordoba", "Argentina", "es"),
    ("Rosario", "Argentina", "es"),
    # Colombia
    ("Bogota", "Colombia", "es"),
    ("Medellin", "Colombia", "es"),
    ("Cartagena", "Colombia", "es"),
    # Chile
    ("Santiago", "Chile", "es"),
    ("Valparaiso", "Chile", "es"),
    # Peru
    ("Lima", "Peru", "es"),
    # Uruguay
    ("Montevideo", "Uruguay", "es"),
    # Ecuador
    ("Quito", "Ecuador", "es"),
    ("Guayaquil", "Ecuador", "es"),
    # Central America & Caribbean
    ("San Jose", "Costa Rica", "es"),
    ("Ciudad de Panama", "Panama", "es"),
    ("Santo Domingo", "Republica Dominicana", "es"),
    ("Guatemala City", "Guatemala", "es"),
    ("San Salvador", "El Salvador", "es"),
]

# --- Apollo Search Settings ---
# Industry tags for marketing agencies in Apollo
APOLLO_INDUSTRY_KEYWORDS = [
    "marketing",
    "advertising",
    "digital marketing",
    "social media",
    "publicidad",
]
# Job titles to search for decision makers
APOLLO_TARGET_TITLES = [
    "CEO",
    "Founder",
    "Co-Founder",
    "Director",
    "Managing Director",
    "Owner",
    "CMO",
    "Marketing Director",
]
APOLLO_RESULTS_PER_PAGE = 25

# --- Directories/Sites to Exclude from Search Results ---
# These are directories, not actual agencies
EXCLUDED_DOMAINS = [
    "clutch.co",
    "sortlist.com",
    "goodfirms.co",
    "designrush.com",
    "agencyspotter.com",
    "upcity.com",
    "g2.com",
    "capterra.com",
    "trustpilot.com",
    "yelp.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "youtube.com",
    "tiktok.com",
    "wikipedia.org",
    "reddit.com",
    "medium.com",
    "hubspot.com",
    "semrush.com",
    "ahrefs.com",
    "neilpatel.com",
    "hootsuite.com",
    "sproutsocial.com",
    "google.com",
]

# --- Email Scraping Patterns ---
# Pages to check for contact info on agency websites
CONTACT_PAGES = [
    "",              # homepage
    "/contacto",
    "/contact",
    "/contact-us",
    "/contactanos",
    "/about",
    "/about-us",
    "/nosotros",
    "/sobre-nosotros",
    "/equipo",
    "/team",
]

# Emails to deprioritize (generic, not personal)
LOW_PRIORITY_EMAIL_PREFIXES = [
    "noreply",
    "no-reply",
    "no.reply",
    "donotreply",
    "mailer-daemon",
    "postmaster",
    "webmaster",
    "admin",
    "support",
    "newsletter",
    "suscripciones",
    "unsubscribe",
]

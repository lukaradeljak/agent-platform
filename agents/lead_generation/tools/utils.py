"""
Shared utilities for the Lead Generation & Enrichment Pipeline.
Logging, retry logic, email validation, text processing.
"""

import functools
import json
import logging
import re
import time
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse

from tools.config import LOG_PATH, TMP_DIR


def setup_logging():
    """Configure logging to both console and rotating file."""
    TMP_DIR.mkdir(exist_ok=True)

    logger = logging.getLogger("pipeline")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler with rotation (5 MB max, keep 3 backups)
    file_handler = RotatingFileHandler(
        LOG_PATH, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def retry(max_attempts=3, backoff_factor=2, exceptions=(Exception,)):
    """Decorator for retrying failed operations with exponential backoff."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger("pipeline")
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts - 1:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise
                    wait = backoff_factor ** attempt
                    logger.warning(
                        f"Retry {attempt + 1}/{max_attempts} for {func.__name__}: {e}. "
                        f"Waiting {wait}s..."
                    )
                    time.sleep(wait)
        return wrapper
    return decorator


def clean_email(raw_email):
    """Validate and normalize an email address. Returns None if invalid."""
    if not raw_email:
        return None
    email = raw_email.strip().lower()
    # Basic email validation regex
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(pattern, email):
        return email
    return None


def extract_domain(url):
    """Extract the root domain from a URL."""
    if not url:
        return None
    try:
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        # Remove www. prefix
        domain = re.sub(r'^www\.', '', domain)
        return domain.lower()
    except Exception:
        return None


def safe_json_parse(text):
    """
    Parse JSON from text that may contain markdown code blocks or extra text.
    Handles responses like: ```json\n{...}\n```
    """
    if not text:
        return None

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try extracting from markdown code block
    patterns = [
        r'```json\s*\n?(.*?)\n?\s*```',
        r'```\s*\n?(.*?)\n?\s*```',
        r'\{[\s\S]*\}',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1) if '```' in pattern else match.group(0))
            except (json.JSONDecodeError, IndexError):
                continue

    return None


def sanitize_text(html_or_text, max_length=1000):
    """Strip HTML tags and limit text length for AI prompt input."""
    if not html_or_text:
        return ""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html_or_text)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove common boilerplate phrases
    for phrase in ["cookie", "privacy policy", "terms of service", "subscribe to our"]:
        text = re.sub(rf'[^.]*{phrase}[^.]*\.?', '', text, flags=re.IGNORECASE)
    # Truncate
    if len(text) > max_length:
        text = text[:max_length].rsplit(' ', 1)[0] + "..."
    return text


def extract_emails_from_text(text):
    """Extract all email addresses from a text string."""
    if not text:
        return []
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    raw_emails = re.findall(pattern, text)
    # Deduplicate while preserving order
    seen = set()
    emails = []
    for email in raw_emails:
        email_lower = email.lower()
        if email_lower not in seen:
            seen.add(email_lower)
            cleaned = clean_email(email)
            if cleaned:
                emails.append(cleaned)
    return emails


def is_excluded_domain(url, excluded_domains):
    """Check if a URL belongs to an excluded domain (directory, social media, etc.)."""
    domain = extract_domain(url)
    if not domain:
        return True
    for excluded in excluded_domains:
        if excluded in domain:
            return True
    return False

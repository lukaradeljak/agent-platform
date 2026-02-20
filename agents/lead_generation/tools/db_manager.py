"""
Persistence layer for the Lead Generation & Enrichment Pipeline.

Supports:
- SQLite (default, local file)
- Supabase Postgres (when SUPABASE_DB_URL / DATABASE_URL is configured)
"""

import json
import sqlite3
from datetime import date, datetime

try:
    import psycopg  # psycopg v3
except ImportError:  # pragma: no cover - optional dependency in SQLite mode
    psycopg = None

from tools.config import CITIES, DB_PATH, FOLLOWUP_DAYS, LEADS_PER_DAY, SUPABASE_DB_URL, TMP_DIR


def _using_postgres() -> bool:
    return bool(SUPABASE_DB_URL)


def _adapt_query(query: str) -> str:
    """Convert SQLite-style ? placeholders to Postgres %s when needed."""
    if _using_postgres():
        return query.replace("?", "%s")
    return query


def _execute(cursor, query: str, params=None):
    if params is None:
        return cursor.execute(_adapt_query(query))
    return cursor.execute(_adapt_query(query), params)


def _fetchall_dicts(cursor) -> list[dict]:
    rows = cursor.fetchall()
    if not rows:
        return []

    first = rows[0]
    if isinstance(first, dict):
        return rows
    if hasattr(first, "keys"):  # sqlite3.Row
        return [dict(row) for row in rows]

    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def _fetchone_dict(cursor) -> dict | None:
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):  # sqlite3.Row
        return dict(row)
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def _placeholder_list(count: int) -> str:
    return ",".join("?" for _ in range(count))


def _get_connection():
    """Get a DB connection (SQLite or Postgres)."""
    TMP_DIR.mkdir(exist_ok=True)

    if _using_postgres():
        if psycopg is None:
            raise RuntimeError(
                "SUPABASE_DB_URL is configured but psycopg is not installed. "
                "Install psycopg[binary] to use Supabase Postgres."
            )
        return psycopg.connect(SUPABASE_DB_URL)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create tables if they don't exist and seed city rotation."""
    conn = _get_connection()
    cursor = conn.cursor()

    if _using_postgres():
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id BIGSERIAL PRIMARY KEY,
                domain TEXT UNIQUE,
                company_name TEXT NOT NULL,
                website TEXT,
                phone TEXT,
                address TEXT,
                city TEXT,
                country TEXT,
                snippet TEXT,
                contact_name TEXT,
                email TEXT,
                email_source TEXT,
                scraped_text TEXT,
                ai_summary TEXT,
                automation_suggestions TEXT,
                discovered_date TEXT,
                sent_date TEXT,
                status TEXT DEFAULT 'new'
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS city_rotation (
                id BIGSERIAL PRIMARY KEY,
                city_name TEXT NOT NULL,
                country TEXT NOT NULL,
                language TEXT DEFAULT 'es',
                last_searched TEXT,
                search_count INTEGER DEFAULT 0,
                UNIQUE(city_name, country)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id BIGSERIAL PRIMARY KEY,
                run_date TEXT,
                leads_discovered INTEGER DEFAULT 0,
                leads_enriched INTEGER DEFAULT 0,
                leads_with_email INTEGER DEFAULT 0,
                leads_ai_analyzed INTEGER DEFAULT 0,
                leads_sent INTEGER DEFAULT 0,
                outreach_sent INTEGER DEFAULT 0,
                errors TEXT,
                duration_seconds REAL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS outreach (
                id BIGSERIAL PRIMARY KEY,
                lead_id BIGINT NOT NULL,
                email_to TEXT NOT NULL,
                email_subject TEXT,
                email_body TEXT,
                outreach_type TEXT DEFAULT 'initial',
                sent_date TEXT,
                gmass_message_id TEXT,
                opened INTEGER DEFAULT 0,
                clicked INTEGER DEFAULT 0,
                replied INTEGER DEFAULT 0,
                reply_date TEXT,
                followup_sent INTEGER DEFAULT 0,
                followup_date TEXT,
                status TEXT DEFAULT 'pending',
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            )
            """
        )
    else:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT UNIQUE,
                company_name TEXT NOT NULL,
                website TEXT,
                phone TEXT,
                address TEXT,
                city TEXT,
                country TEXT,
                snippet TEXT,
                contact_name TEXT,
                email TEXT,
                email_source TEXT,
                scraped_text TEXT,
                ai_summary TEXT,
                automation_suggestions TEXT,
                discovered_date TEXT,
                sent_date TEXT,
                status TEXT DEFAULT 'new'
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS city_rotation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city_name TEXT NOT NULL,
                country TEXT NOT NULL,
                language TEXT DEFAULT 'es',
                last_searched TEXT,
                search_count INTEGER DEFAULT 0,
                UNIQUE(city_name, country)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT,
                leads_discovered INTEGER DEFAULT 0,
                leads_enriched INTEGER DEFAULT 0,
                leads_with_email INTEGER DEFAULT 0,
                leads_ai_analyzed INTEGER DEFAULT 0,
                leads_sent INTEGER DEFAULT 0,
                outreach_sent INTEGER DEFAULT 0,
                errors TEXT,
                duration_seconds REAL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS outreach (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                email_to TEXT NOT NULL,
                email_subject TEXT,
                email_body TEXT,
                outreach_type TEXT DEFAULT 'initial',
                sent_date TEXT,
                gmass_message_id TEXT,
                opened INTEGER DEFAULT 0,
                clicked INTEGER DEFAULT 0,
                replied INTEGER DEFAULT 0,
                reply_date TEXT,
                followup_sent INTEGER DEFAULT 0,
                followup_date TEXT,
                status TEXT DEFAULT 'pending',
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            )
            """
        )

    # Backfill schema changes for existing databases.
    if _using_postgres():
        _execute(
            cursor,
            "ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS outreach_sent INTEGER DEFAULT 0",
        )
    else:
        cursor.execute("PRAGMA table_info(pipeline_runs)")
        cols = [row[1] for row in cursor.fetchall()]
        if "outreach_sent" not in cols:
            cursor.execute("ALTER TABLE pipeline_runs ADD COLUMN outreach_sent INTEGER DEFAULT 0")

    cursor.execute("SELECT COUNT(*) FROM city_rotation")
    city_count = cursor.fetchone()[0]
    if city_count == 0:
        for city_name, country, lang in CITIES:
            if _using_postgres():
                _execute(
                    cursor,
                    """
                    INSERT INTO city_rotation (city_name, country, language)
                    VALUES (?, ?, ?)
                    ON CONFLICT (city_name, country) DO NOTHING
                    """,
                    (city_name, country, lang),
                )
            else:
                _execute(
                    cursor,
                    """
                    INSERT OR IGNORE INTO city_rotation (city_name, country, language)
                    VALUES (?, ?, ?)
                    """,
                    (city_name, country, lang),
                )

    conn.commit()
    conn.close()


def lead_exists(domain):
    """Check if a lead with this domain already exists."""
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(cursor, "SELECT id FROM leads WHERE domain = ?", (domain,))
    result = cursor.fetchone()
    conn.close()
    return result is not None


def insert_lead(lead_data):
    """
    Insert a new lead. Returns the lead id or None if duplicate.

    lead_data: dict with keys matching the leads table columns.
    """
    conn = _get_connection()
    cursor = conn.cursor()
    try:
        params = (
            lead_data.get("domain"),
            lead_data.get("company_name"),
            lead_data.get("website"),
            lead_data.get("phone"),
            lead_data.get("address"),
            lead_data.get("city"),
            lead_data.get("country"),
            lead_data.get("snippet"),
            lead_data.get("contact_name"),
            lead_data.get("email"),
            lead_data.get("email_source"),
            datetime.now().strftime("%Y-%m-%d"),
        )

        if _using_postgres():
            _execute(
                cursor,
                """
                INSERT INTO leads
                (domain, company_name, website, phone, address, city, country,
                 snippet, contact_name, email, email_source, discovered_date, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new')
                RETURNING id
                """,
                params,
            )
            lead_id = cursor.fetchone()[0]
        else:
            _execute(
                cursor,
                """
                INSERT INTO leads
                (domain, company_name, website, phone, address, city, country,
                 snippet, contact_name, email, email_source, discovered_date, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new')
                """,
                params,
            )
            lead_id = cursor.lastrowid

        conn.commit()
        conn.close()
        return lead_id
    except sqlite3.IntegrityError:
        conn.close()
        return None
    except Exception as exc:
        if _using_postgres() and "duplicate key value violates unique constraint" in str(exc).lower():
            conn.rollback()
            conn.close()
            return None
        conn.close()
        raise


def insert_leads_batch(leads_list):
    """Insert multiple leads. Returns count of successfully inserted."""
    inserted = 0
    for lead in leads_list:
        if insert_lead(lead) is not None:
            inserted += 1
    return inserted


def get_leads_needing_enrichment(limit=None):
    """Get leads that need website scraping (no email or no scraped content for AI)."""
    if limit is None:
        limit = LEADS_PER_DAY
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        SELECT id, company_name, website, city, country, email
        FROM leads
        WHERE (email IS NULL OR scraped_text IS NULL)
          AND website IS NOT NULL AND status = 'new'
        ORDER BY
            CASE WHEN email IS NULL THEN 0 ELSE 1 END,
            discovered_date DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = _fetchall_dicts(cursor)
    conn.close()
    return rows


def get_leads_needing_email_enrichment(limit=None):
    """Get leads that still have no email and should be retried for enrichment.

    Note: Do not filter by email_source here. Historically, some leads could
    end up with email_source='apollo' but email still NULL (e.g., a match that
    returned name/title/phone but no email). Filtering by email_source would
    permanently exclude those leads from future enrichment attempts, causing
    a persistent shortfall in emails delivered.
    """
    if limit is None:
        limit = LEADS_PER_DAY
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        SELECT id, company_name, website, domain, city, country, contact_name
        FROM leads
        WHERE email IS NULL
          AND website IS NOT NULL
          AND status = 'new'
        ORDER BY discovered_date DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = _fetchall_dicts(cursor)
    conn.close()
    return rows


def get_leads_missing_phone(limit=None):
    """Get leads missing a phone number."""
    if limit is None:
        limit = LEADS_PER_DAY
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        SELECT id, domain, company_name
        FROM leads
        WHERE phone IS NULL
          AND domain IS NOT NULL
          AND status = 'new'
        ORDER BY discovered_date DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = _fetchall_dicts(cursor)
    conn.close()
    return rows


def get_leads_needing_ai(limit=None):
    """Get leads that need AI analysis."""
    if limit is None:
        limit = LEADS_PER_DAY
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        SELECT id, company_name, website, domain, city, country,
               phone, snippet, scraped_text, email, contact_name
        FROM leads
        WHERE ai_summary IS NULL AND status = 'new'
        ORDER BY discovered_date DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = _fetchall_dicts(cursor)
    conn.close()
    return rows


def update_lead_enrichment(lead_id, data):
    """
    Update a lead with enrichment data.

    data: dict with any of: email, email_source, contact_name, scraped_text,
          phone, address
    """
    conn = _get_connection()
    cursor = conn.cursor()
    set_clauses = []
    values = []

    for key in ["email", "email_source", "contact_name", "scraped_text", "phone", "address"]:
        if key in data and data[key] is not None:
            set_clauses.append(f"{key} = ?")
            values.append(data[key])

    if set_clauses:
        values.append(lead_id)
        _execute(
            cursor,
            f"UPDATE leads SET {', '.join(set_clauses)} WHERE id = ?",
            values,
        )
        conn.commit()

    conn.close()


def update_lead_ai(lead_id, ai_summary, automation_suggestions):
    """Update a lead with AI analysis results."""
    conn = _get_connection()
    cursor = conn.cursor()
    suggestions_json = (
        json.dumps(automation_suggestions, ensure_ascii=False)
        if isinstance(automation_suggestions, (list, dict))
        else automation_suggestions
    )
    _execute(
        cursor,
        "UPDATE leads SET ai_summary = ?, automation_suggestions = ? WHERE id = ?",
        (ai_summary, suggestions_json, lead_id),
    )
    conn.commit()
    conn.close()


def get_unsent_leads(limit=None):
    """Get enriched leads that haven't been sent yet, prioritizing those with emails."""
    if limit is None:
        limit = LEADS_PER_DAY
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        SELECT id, domain, company_name, website, phone, address,
               city, country, contact_name, email, email_source,
               ai_summary, automation_suggestions, discovered_date
        FROM leads
        WHERE sent_date IS NULL AND status = 'new' AND ai_summary IS NOT NULL
        ORDER BY
            CASE
                WHEN email IS NOT NULL AND phone IS NOT NULL THEN 0
                WHEN email IS NOT NULL THEN 1
                WHEN phone IS NOT NULL THEN 2
                ELSE 3
            END,
            discovered_date DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = _fetchall_dicts(cursor)
    conn.close()
    return rows


def mark_leads_sent(lead_ids, sent_date=None):
    """Mark leads as sent."""
    if not lead_ids:
        return

    if sent_date is None:
        sent_date = date.today().isoformat()

    conn = _get_connection()
    cursor = conn.cursor()
    placeholders = _placeholder_list(len(lead_ids))
    _execute(
        cursor,
        f"UPDATE leads SET sent_date = ?, status = 'sent' WHERE id IN ({placeholders})",
        [sent_date] + list(lead_ids),
    )
    conn.commit()
    conn.close()


def get_next_city():
    """Get the next city to search (least recently searched)."""
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        SELECT city_name, country, language
        FROM city_rotation
        ORDER BY
            CASE WHEN last_searched IS NULL THEN '1900-01-01' ELSE last_searched END ASC,
            search_count ASC
        LIMIT 1
        """
    )
    row = _fetchone_dict(cursor)
    conn.close()
    return row


def update_city_searched(city_name, country):
    """Mark a city as searched today."""
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        UPDATE city_rotation
        SET last_searched = ?, search_count = search_count + 1
        WHERE city_name = ? AND country = ?
        """,
        (date.today().isoformat(), city_name, country),
    )
    conn.commit()
    conn.close()


def reset_city_rotation(start_city="Madrid", start_country=None):
    """
    Reset city rotation so the next discovery starts from a specific city.

    Strategy:
    - Mark all cities as recently searched (today)
    - Set target city as not searched (NULL, count=0), so it becomes next
    """
    conn = _get_connection()
    cursor = conn.cursor()

    today = date.today().isoformat()
    _execute(
        cursor,
        "UPDATE city_rotation SET last_searched = ?, search_count = 1",
        (today,),
    )

    if start_country:
        _execute(
            cursor,
            """
            UPDATE city_rotation
            SET last_searched = NULL, search_count = 0
            WHERE city_name = ? AND country = ?
            """,
            (start_city, start_country),
        )
    else:
        _execute(
            cursor,
            """
            UPDATE city_rotation
            SET last_searched = NULL, search_count = 0
            WHERE city_name = ?
            """,
            (start_city,),
        )

    updated = cursor.rowcount if cursor.rowcount is not None else 0
    conn.commit()
    conn.close()
    return updated > 0


def log_pipeline_run(stats):
    """Record a pipeline run with its stats."""
    conn = _get_connection()
    cursor = conn.cursor()
    errors_json = json.dumps(stats.get("errors", []), ensure_ascii=False) if stats.get("errors") else None
    _execute(
        cursor,
        """
        INSERT INTO pipeline_runs
        (run_date, leads_discovered, leads_enriched, leads_with_email,
         leads_ai_analyzed, leads_sent, outreach_sent, errors, duration_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            stats.get("discovered", 0),
            stats.get("enriched", 0),
            stats.get("with_email", 0),
            stats.get("ai_analyzed", 0),
            stats.get("sent", 0),
            stats.get("outreach_sent", 0),
            errors_json,
            stats.get("duration_seconds", 0),
        ),
    )
    conn.commit()
    conn.close()


def get_total_leads_count():
    """Get total number of leads in the database."""
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(cursor, "SELECT COUNT(*) FROM leads")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_unsent_count():
    """Get count of leads that are enriched but not yet sent."""
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        "SELECT COUNT(*) FROM leads WHERE sent_date IS NULL AND status = 'new' AND ai_summary IS NOT NULL",
    )
    count = cursor.fetchone()[0]
    conn.close()
    return count


# --- Outreach Functions ---


def get_leads_for_outreach(limit=None):
    """Get leads that have been sent in report but not yet contacted individually."""
    if limit is None:
        limit = LEADS_PER_DAY
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        SELECT l.id, l.domain, l.company_name, l.website, l.phone,
               l.city, l.country, l.contact_name, l.email,
               l.ai_summary, l.automation_suggestions, l.sent_date
        FROM leads l
        LEFT JOIN outreach o ON l.id = o.lead_id AND o.outreach_type = 'initial'
        WHERE l.sent_date IS NOT NULL
          AND l.email IS NOT NULL
          AND o.id IS NULL
        ORDER BY l.sent_date DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = _fetchall_dicts(cursor)
    conn.close()
    return rows


def insert_outreach(lead_id, email_to, subject, body, outreach_type="initial", gmass_id=None):
    """Record an outreach email sent."""
    conn = _get_connection()
    cursor = conn.cursor()

    if _using_postgres():
        _execute(
            cursor,
            """
            INSERT INTO outreach
            (lead_id, email_to, email_subject, email_body, outreach_type,
             sent_date, gmass_message_id, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'sent')
            RETURNING id
            """,
            (
                lead_id,
                email_to,
                subject,
                body,
                outreach_type,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                gmass_id,
            ),
        )
        outreach_id = cursor.fetchone()[0]
    else:
        _execute(
            cursor,
            """
            INSERT INTO outreach
            (lead_id, email_to, email_subject, email_body, outreach_type,
             sent_date, gmass_message_id, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'sent')
            """,
            (
                lead_id,
                email_to,
                subject,
                body,
                outreach_type,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                gmass_id,
            ),
        )
        outreach_id = cursor.lastrowid

    conn.commit()
    conn.close()
    return outreach_id


def get_outreach_needing_followup():
    """Get outreach emails needing followup (sent X days ago, no reply, no followup yet)."""
    conn = _get_connection()
    cursor = conn.cursor()

    if _using_postgres():
        _execute(
            cursor,
            """
            SELECT o.id, o.lead_id, o.email_to, o.email_subject, o.email_body,
                   l.company_name, l.contact_name, l.ai_summary, l.automation_suggestions
            FROM outreach o
            JOIN leads l ON o.lead_id = l.id
            WHERE o.outreach_type = 'initial'
              AND o.replied = 0
              AND o.followup_sent = 0
              AND o.status = 'sent'
              AND CAST(o.sent_date AS DATE) <= CURRENT_DATE - (? * INTERVAL '1 day')
            ORDER BY o.sent_date ASC
            """,
            (FOLLOWUP_DAYS,),
        )
    else:
        _execute(
            cursor,
            f"""
            SELECT o.id, o.lead_id, o.email_to, o.email_subject, o.email_body,
                   l.company_name, l.contact_name, l.ai_summary, l.automation_suggestions
            FROM outreach o
            JOIN leads l ON o.lead_id = l.id
            WHERE o.outreach_type = 'initial'
              AND o.replied = 0
              AND o.followup_sent = 0
              AND o.status = 'sent'
              AND date(o.sent_date) <= date('now', '-{FOLLOWUP_DAYS} days')
            ORDER BY o.sent_date ASC
            """,
        )

    rows = _fetchall_dicts(cursor)
    conn.close()
    return rows


def mark_followup_sent(outreach_id, followup_outreach_id=None):
    """Mark that a followup was sent for an outreach."""
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        UPDATE outreach
        SET followup_sent = 1, followup_date = ?
        WHERE id = ?
        """,
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), outreach_id),
    )
    conn.commit()
    conn.close()


def mark_outreach_replied(outreach_id):
    """Mark an outreach as replied (manual or webhook-driven)."""
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        UPDATE outreach
        SET replied = 1, reply_date = ?, status = 'replied'
        WHERE id = ?
        """,
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), outreach_id),
    )
    conn.commit()
    conn.close()


def get_outreach_stats():
    """Get outreach statistics."""
    conn = _get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        """
        SELECT
            COUNT(*) as total_sent,
            SUM(CASE WHEN outreach_type = 'initial' THEN 1 ELSE 0 END) as initial_sent,
            SUM(CASE WHEN outreach_type = 'followup' THEN 1 ELSE 0 END) as followups_sent,
            SUM(replied) as total_replied,
            SUM(opened) as total_opened
        FROM outreach
        """,
    )
    row = _fetchone_dict(cursor)
    conn.close()
    return row if row else {}

"""
Tarea que sincroniza métricas desde el collector hacia Supabase
llamando al endpoint /api/cron/sync de la app web.
"""

import logging
import os

import requests

from scheduler.celery_app import app

log = logging.getLogger(__name__)


@app.task(name="scheduler.tasks.sync.sync_platform_to_supabase")
def sync_platform_to_supabase() -> dict:
    """Llama al endpoint de sync de la app web para volcar métricas a Supabase."""
    url = os.environ.get("VERCEL_SYNC_URL", "").strip()
    secret = os.environ.get("CRON_SECRET", "").strip()

    if not url:
        log.warning("VERCEL_SYNC_URL no configurada — sync saltado")
        return {"skipped": True, "reason": "VERCEL_SYNC_URL not set"}

    headers = {"Accept": "application/json"}
    if secret:
        headers["Authorization"] = f"Bearer {secret}"

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        log.info(f"Sync OK: {data.get('report', data)}")
        return data
    except requests.RequestException as exc:
        log.error(f"Sync falló: {exc}")
        raise

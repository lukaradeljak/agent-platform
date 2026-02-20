"""
HTTP API server for ACEM sync endpoints.

Runs together with the existing scheduler process so Coolify can expose:
- GET /health
- GET /api/acem/agent-status
- GET /api/acem/agent-events
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from tools import db_manager

CLIENT_EXTERNAL_ID = os.getenv("ACEM_CLIENT_EXTERNAL_ID", "acem_default_client")
CLIENT_NAME = os.getenv("ACEM_CLIENT_NAME", "ACEM Systems")
AGENT_EXTERNAL_ID = os.getenv("ACEM_AGENT_EXTERNAL_ID", "acem_lead_generation")
AGENT_NAME = os.getenv("ACEM_AGENT_NAME", "ACEM lead generation")
CURRENCY_CODE = os.getenv("ACEM_CURRENCY_CODE", "USD")
SCHEDULER_ENABLED = os.getenv("ACEM_SCHEDULER_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _parse_bool(value: str, default: bool = False) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _parse_int(value: str, default: int) -> int:
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_metrics_mock_config() -> tuple[bool, int, int]:
    """
    Temporary testing hook: force inflated metrics without running the pipeline.

    Controlled via env vars:
    - ACEM_METRICS_MOCK=true/false
    - ACEM_METRICS_MOCK_RUNS_TOTAL=80
    - ACEM_METRICS_MOCK_TASKS_COMPLETED=80
    """
    enabled = _parse_bool(os.getenv("ACEM_METRICS_MOCK", "false"))
    runs_total = _parse_int(os.getenv("ACEM_METRICS_MOCK_RUNS_TOTAL", "80"), 80)
    tasks_completed = _parse_int(os.getenv("ACEM_METRICS_MOCK_TASKS_COMPLETED", "80"), 80)
    return enabled, runs_total, tasks_completed


app = FastAPI(title="ACEM Lead Pipeline API", version="1.0.0")
_scheduler_process: subprocess.Popen | None = None


def _parse_iso_datetime(value: str) -> datetime:
    raw = value.strip()
    if not raw:
        raise ValueError("Empty datetime")

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _parse_db_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Empty run_date")

        parsed: datetime | None = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue

        if parsed is None:
            parsed = _parse_iso_datetime(text)
        dt = parsed

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_errors(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]

    text = str(raw).strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except json.JSONDecodeError:
        pass

    return [text]


def _status_from_errors(error_count: int) -> str:
    if error_count == 0:
        return "Activo"
    if error_count <= 2:
        return "Optimizando"
    return "En revision"


def _severity_from_error(error_text: str) -> str:
    value = error_text.lower()
    if "critical" in value or "fatal" in value or "traceback" in value:
        return "critical"
    return "warning"


def _get_pipeline_runs(updated_after: datetime) -> list[dict[str, Any]]:
    conn = db_manager._get_connection()
    cursor = conn.cursor()
    try:
        db_manager._execute(
            cursor,
            """
            SELECT run_date, leads_discovered, leads_enriched, leads_with_email,
                   leads_ai_analyzed, leads_sent, outreach_sent, errors, duration_seconds
            FROM pipeline_runs
            WHERE run_date >= ?
            ORDER BY run_date ASC
            """,
            (updated_after.strftime("%Y-%m-%d %H:%M:%S"),),
        )
        return db_manager._fetchall_dicts(cursor)
    finally:
        conn.close()


def _query_single_int(cursor, query: str, params: tuple[Any, ...] = ()) -> int:
    db_manager._execute(cursor, query, params)
    row = cursor.fetchone()
    if row is None:
        return 0

    try:
        return int(row[0] or 0)
    except (TypeError, ValueError):
        return 0


def _count_outreach_sent_between(start_dt: datetime, end_dt: datetime) -> int:
    conn = db_manager._get_connection()
    cursor = conn.cursor()
    try:
        return _query_single_int(
            cursor,
            """
            SELECT COUNT(*)
            FROM outreach
            WHERE sent_date IS NOT NULL
              AND outreach_type = 'initial'
              AND sent_date >= ?
              AND sent_date < ?
            """,
            (
                start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
    finally:
        conn.close()


def _build_snapshot_rows(updated_after: datetime) -> tuple[dict[str, Any], dict[str, Any]] | None:
    now = datetime.now(timezone.utc)
    if now < updated_after:
        return None

    conn = db_manager._get_connection()
    cursor = conn.cursor()
    try:
        total_leads = _query_single_int(cursor, "SELECT COUNT(*) FROM leads")
        sent_total = _query_single_int(cursor, "SELECT COUNT(*) FROM leads WHERE sent_date IS NOT NULL")
        pending_total = _query_single_int(cursor, "SELECT COUNT(*) FROM leads WHERE sent_date IS NULL")
        outreach_total = _query_single_int(
            cursor,
            "SELECT COUNT(*) FROM outreach WHERE sent_date IS NOT NULL AND outreach_type = 'initial'",
        )

        db_manager._execute(
            cursor,
            """
            SELECT run_date, leads_discovered, leads_sent, outreach_sent, errors, duration_seconds
            FROM pipeline_runs
            ORDER BY run_date DESC
            LIMIT 1
            """,
        )
        latest_run = db_manager._fetchone_dict(cursor)
    finally:
        conn.close()

    mock_enabled, mock_runs_total, mock_tasks_completed = _get_metrics_mock_config()
    if mock_enabled:
        total_leads = max(mock_runs_total, 0)
        sent_total = max(mock_tasks_completed, 0)
        pending_total = max(total_leads - sent_total, 0)

    latest_errors = _parse_errors(latest_run.get("errors")) if latest_run else []
    latest_discovered = int((latest_run or {}).get("leads_discovered") or 0)
    latest_sent = int((latest_run or {}).get("leads_sent") or 0)
    latest_outreach = int((latest_run or {}).get("outreach_sent") or 0)
    latest_duration = float((latest_run or {}).get("duration_seconds") or 0)

    if mock_enabled:
        latest_discovered = total_leads
        latest_sent = sent_total

    status_value = (
        _status_from_errors(len(latest_errors))
        if latest_run
        else ("Activo" if sent_total > 0 else "Optimizando")
    )

    # Round to 10-minute buckets to keep idempotent upserts by bucket.
    bucket_dt = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
    bucket_iso = _to_iso_z(bucket_dt)
    now_iso = _to_iso_z(now)

    status_row = {
        "client_external_id": CLIENT_EXTERNAL_ID,
        "client_name": CLIENT_NAME,
        "agent_external_id": AGENT_EXTERNAL_ID,
        "agent_name": AGENT_NAME,
        "status": status_value,
        "bucket_start": bucket_iso,
        # One row represents one pipeline execution.
        "runs_total": 1,
        "success_rate": round(max(0.0, 100.0 - min(100.0, len(latest_errors) * 25.0)), 2),
        "avg_latency_ms": round(max(latest_duration, 0.0) * 1000, 2),
        # Use outreach emails (initial) as the real "mails sent" indicator.
        "tasks_completed": max(latest_outreach, 0),
        "est_impact_value": 0.0,
        "currency_code": CURRENCY_CODE,
        "updated_at": now_iso,
    }

    event_row = {
        "client_external_id": CLIENT_EXTERNAL_ID,
        "client_name": CLIENT_NAME,
        "agent_external_id": AGENT_EXTERNAL_ID,
        "agent_name": AGENT_NAME,
        "status": status_value,
        "source_event_id": f"snapshot:{bucket_iso}",
        "occurred_at": now_iso,
        "updated_at": now_iso,
        "event_type": "pipeline_snapshot",
        "severity": "info",
        "title": "Resumen operativo actualizado",
        "summary": (
            f"Leads totales: {total_leads}. Informes enviados: {sent_total}. "
            f"Correos enviados: {max(latest_outreach, 0)}. "
            f"Pendientes: {pending_total}. Errores recientes: {len(latest_errors)}."
        ),
        "payload_json": {
            "leads_total": total_leads,
            "leads_sent_total": sent_total,
            "outreach_sent_total": outreach_total,
            "leads_pending_total": pending_total,
            "latest_run_errors": len(latest_errors),
            "latest_run_discovered": latest_discovered,
            "latest_run_sent": latest_sent,
            "latest_run_outreach_sent": latest_outreach,
        },
    }

    return status_row, event_row


def _build_status_rows(updated_after: datetime) -> list[dict[str, Any]]:
    rows = _get_pipeline_runs(updated_after)
    parsed_rows: list[tuple[dict[str, Any], datetime]] = []
    for row in rows:
        try:
            parsed_rows.append((row, _parse_db_datetime(row.get("run_date"))))
        except ValueError:
            continue

    output: list[dict[str, Any]] = []
    mock_enabled, mock_runs_total, mock_tasks_completed = _get_metrics_mock_config()

    for index, (row, run_dt) in enumerate(parsed_rows):
        errors = _parse_errors(row.get("errors"))
        error_count = len(errors)

        duration_seconds = float(row.get("duration_seconds") or 0)
        discovered = int(row.get("leads_discovered") or 0)
        outreach_sent = int(row.get("outreach_sent") or 0)

        if mock_enabled:
            discovered = max(mock_runs_total, 0)
            outreach_sent = max(mock_tasks_completed, 0)
        elif outreach_sent <= 0:
            next_dt = parsed_rows[index + 1][1] if index + 1 < len(parsed_rows) else datetime.now(timezone.utc)
            outreach_sent = _count_outreach_sent_between(run_dt, next_dt)

        # Bucket to 10-minute intervals.
        bucket_dt = run_dt.replace(minute=(run_dt.minute // 10) * 10, second=0, microsecond=0)
        success_rate = max(0.0, 100.0 - min(100.0, error_count * 25.0))

        output.append(
            {
                "client_external_id": CLIENT_EXTERNAL_ID,
                "client_name": CLIENT_NAME,
                "agent_external_id": AGENT_EXTERNAL_ID,
                "agent_name": AGENT_NAME,
                "status": _status_from_errors(error_count),
                "bucket_start": _to_iso_z(bucket_dt),
                # Each row represents one pipeline execution.
                "runs_total": 1,
                "success_rate": round(success_rate, 2),
                "avg_latency_ms": round(max(duration_seconds, 0) * 1000, 2),
                "tasks_completed": max(outreach_sent, 0),
                "est_impact_value": 0.0,
                "currency_code": CURRENCY_CODE,
                "updated_at": _to_iso_z(run_dt),
            }
        )

    if output:
        return output

    snapshot = _build_snapshot_rows(updated_after)
    if snapshot is None:
        return []

    status_row, _event_row = snapshot
    return [status_row]


def _build_event_rows(updated_after: datetime) -> list[dict[str, Any]]:
    rows = _get_pipeline_runs(updated_after)
    parsed_rows: list[tuple[dict[str, Any], datetime]] = []
    for row in rows:
        try:
            parsed_rows.append((row, _parse_db_datetime(row.get("run_date"))))
        except ValueError:
            continue

    events: list[dict[str, Any]] = []
    mock_enabled, mock_runs_total, mock_tasks_completed = _get_metrics_mock_config()

    for index, (row, run_dt) in enumerate(parsed_rows):
        run_iso = _to_iso_z(run_dt)
        errors = _parse_errors(row.get("errors"))
        outreach_sent = int(row.get("outreach_sent") or 0)
        discovered = int(row.get("leads_discovered") or 0)

        if mock_enabled:
            discovered = max(mock_runs_total, 0)
            outreach_sent = max(mock_tasks_completed, 0)
        elif outreach_sent <= 0:
            next_dt = parsed_rows[index + 1][1] if index + 1 < len(parsed_rows) else datetime.now(timezone.utc)
            outreach_sent = _count_outreach_sent_between(run_dt, next_dt)

        events.append(
            {
                "client_external_id": CLIENT_EXTERNAL_ID,
                "client_name": CLIENT_NAME,
                "agent_external_id": AGENT_EXTERNAL_ID,
                "agent_name": AGENT_NAME,
                "status": _status_from_errors(len(errors)),
                "source_event_id": f"run:{run_iso}:summary",
                "occurred_at": run_iso,
                "updated_at": run_iso,
                "event_type": "pipeline_run",
                "severity": "info",
                "title": "Ejecucion de pipeline completada",
                "summary": (
                    f"Leads descubiertos: {discovered}. Correos enviados: {outreach_sent}. "
                    f"Errores: {len(errors)}."
                ),
                "payload_json": {
                    "leads_discovered": discovered,
                    "leads_enriched": int(row.get("leads_enriched") or 0),
                    "leads_with_email": int(row.get("leads_with_email") or 0),
                    "leads_ai_analyzed": int(row.get("leads_ai_analyzed") or 0),
                    "outreach_sent": outreach_sent,
                    "duration_seconds": float(row.get("duration_seconds") or 0),
                },
            }
        )

        for index, error_text in enumerate(errors):
            severity = _severity_from_error(error_text)
            events.append(
                {
                    "client_external_id": CLIENT_EXTERNAL_ID,
                    "client_name": CLIENT_NAME,
                    "agent_external_id": AGENT_EXTERNAL_ID,
                    "agent_name": AGENT_NAME,
                    "status": _status_from_errors(len(errors)),
                    "source_event_id": f"run:{run_iso}:error:{index}",
                    "occurred_at": run_iso,
                    "updated_at": run_iso,
                    "event_type": "pipeline_error",
                    "severity": severity,
                    "title": "Error reportado en pipeline",
                    "summary": error_text,
                    "payload_json": {"error": error_text},
                }
            )

    if events:
        return events

    snapshot = _build_snapshot_rows(updated_after)
    if snapshot is None:
        return []

    _status_row, event_row = snapshot
    return [event_row]


def _start_scheduler() -> None:
    global _scheduler_process
    if not SCHEDULER_ENABLED or _scheduler_process is not None:
        return

    _scheduler_process = subprocess.Popen(
        [sys.executable, "-m", "tools.scheduler"],
        env=os.environ.copy(),
    )


def _stop_scheduler() -> None:
    global _scheduler_process
    if _scheduler_process is None:
        return

    if _scheduler_process.poll() is None:
        _scheduler_process.terminate()
        try:
            _scheduler_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _scheduler_process.kill()
            _scheduler_process.wait(timeout=5)

    _scheduler_process = None


@app.on_event("startup")
def _on_startup() -> None:
    db_manager.init_db()
    _start_scheduler()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    _stop_scheduler()


@app.get("/health")
def health() -> dict[str, Any]:
    scheduler_running = _scheduler_process is not None and _scheduler_process.poll() is None
    return {
        "status": "ok",
        "service": "acem-lead-pipeline-api",
        "scheduler_enabled": SCHEDULER_ENABLED,
        "scheduler_running": scheduler_running,
        # Non-sensitive scheduler config to debug "didn't run at 09:00" issues.
        "schedule": {
            "tz": os.getenv("TZ", "UTC"),
            "time": (os.getenv("SCHEDULE_TIME_OVERRIDE", "").strip() or os.getenv("SCHEDULE_TIME", "09:00")),
            "days": os.getenv("SCHEDULE_DAYS", "1-5"),
            "catchup_on_boot": os.getenv("SCHEDULE_CATCHUP_ON_BOOT", "false"),
            "run_on_startup": os.getenv("RUN_ON_STARTUP", "false"),
        },
    }


@app.get("/api/acem/agent-status")
def agent_status(updated_after: str = Query("1970-01-01T00:00:00.000Z")) -> list[dict[str, Any]]:
    try:
        cursor = _parse_iso_datetime(updated_after)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid updated_after: {exc}") from exc

    return _build_status_rows(cursor)


@app.get("/api/acem/agent-events")
def agent_events(updated_after: str = Query("1970-01-01T00:00:00.000Z")) -> list[dict[str, Any]]:
    try:
        cursor = _parse_iso_datetime(updated_after)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid updated_after: {exc}") from exc

    return _build_event_rows(cursor)


@app.post("/api/acem/run-now")
def run_now() -> dict[str, Any]:
    from tools.run_pipeline import main as run_pipeline

    started_at = datetime.now(timezone.utc)
    try:
        run_pipeline()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {exc}") from exc

    finished_at = datetime.now(timezone.utc)
    return {
        "ok": True,
        "started_at": _to_iso_z(started_at),
        "finished_at": _to_iso_z(finished_at),
    }


def _handle_signal(signum, _frame) -> None:
    _stop_scheduler()
    raise SystemExit(f"Received signal {signum}")


def main() -> None:
    import uvicorn

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "80"))
    uvicorn.run("tools.server:app", host=host, port=port, workers=1)


if __name__ == "__main__":
    main()

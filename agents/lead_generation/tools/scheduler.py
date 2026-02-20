"""
Container-friendly scheduler for the lead pipeline.

This is intended for Coolify/server deployments where Modal or Windows Task
Scheduler are not used.
"""

import argparse
import logging
import os
import signal
import time
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from tools import db_manager
from tools.run_pipeline import main as run_pipeline
from tools.utils import setup_logging

logger = logging.getLogger("pipeline")
_shutdown_requested = False


def _handle_signal(signum, _frame):
    """Graceful shutdown handler for container stop signals."""
    global _shutdown_requested
    _shutdown_requested = True
    logger.info(f"Scheduler received signal {signum}. Shutting down...")


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_schedule_time(value: str) -> dt_time:
    """Parse HH:MM (24h) schedule format."""
    try:
        hour_str, minute_str = value.split(":")
        hour = int(hour_str)
        minute = int(minute_str)
    except ValueError as exc:
        raise ValueError(f"Invalid SCHEDULE_TIME '{value}'. Use HH:MM format.") from exc

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Invalid SCHEDULE_TIME '{value}'. Hour/minute out of range.")
    return dt_time(hour=hour, minute=minute)


def _expand_day_range(start: int, end: int) -> set[int]:
    """Expand weekday ranges with support for wrapped ranges (e.g., 5-1)."""
    if start <= end:
        return set(range(start, end + 1))
    return set(range(start, 8)) | set(range(1, end + 1))


def _parse_schedule_days(value: str) -> set[int]:
    """
    Parse ISO weekday set from:
    - "*" for all days
    - "1-5" for weekdays
    - "1,3,5" for explicit list
    - combinations: "1-5,7"
    """
    raw = value.strip()
    if raw == "*":
        return set(range(1, 8))

    allowed: set[int] = set()
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue

        if "-" in token:
            start_str, end_str = token.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            if not (1 <= start <= 7 and 1 <= end <= 7):
                raise ValueError(f"Invalid day range '{token}'. Use values 1-7.")
            allowed |= _expand_day_range(start, end)
            continue

        day = int(token)
        if not (1 <= day <= 7):
            raise ValueError(f"Invalid day '{token}'. Use values 1-7.")
        allowed.add(day)

    if not allowed:
        raise ValueError("SCHEDULE_DAYS resolved to an empty set.")
    return allowed


def _get_timezone(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        logger.warning(f"Timezone '{tz_name}' not found. Falling back to UTC.")
        return ZoneInfo("UTC")


def _parse_city_reset_target(value: str) -> tuple[str, str | None]:
    """
    Parse CITY_ROTATION_RESET_TO from:
    - "Madrid"
    - "Madrid, Espana"
    """
    raw = value.strip()
    if not raw:
        return "", None
    if "," in raw:
        city, country = raw.split(",", 1)
        return city.strip(), country.strip() or None
    return raw, None


def _run_pipeline_once(reason: str) -> None:
    logger.info(f"Scheduler trigger: {reason}. Running pipeline...")
    start = time.time()
    try:
        run_pipeline()
        logger.info(f"Scheduler run complete in {time.time() - start:.1f}s")
    except Exception:
        logger.exception("Scheduler run failed with an unhandled exception.")


def _should_trigger_scheduled_run(
    now: datetime,
    schedule_time: dt_time,
    allowed_days: set[int],
    last_scheduled_run_date,
) -> bool:
    if now.isoweekday() not in allowed_days:
        return False
    if last_scheduled_run_date == now.date():
        return False
    current_hm = (now.hour, now.minute)
    target_hm = (schedule_time.hour, schedule_time.minute)
    return current_hm >= target_hm


def _is_after_schedule(now: datetime, schedule_time: dt_time) -> bool:
    """Return True if current local time is at/after scheduled HH:MM."""
    return (now.hour, now.minute) >= (schedule_time.hour, schedule_time.minute)


def main():
    parser = argparse.ArgumentParser(description="Run lead pipeline scheduler")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the pipeline immediately once and exit.",
    )
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print parsed scheduler config and exit.",
    )
    args = parser.parse_args()

    setup_logging()

    schedule_time_override = os.getenv("SCHEDULE_TIME_OVERRIDE", "").strip()
    schedule_time_raw = schedule_time_override or os.getenv("SCHEDULE_TIME", "09:00")
    schedule_days_raw = os.getenv("SCHEDULE_DAYS", "1-5")
    timezone_raw = os.getenv("TZ", "UTC")
    poll_seconds = int(os.getenv("SCHEDULER_POLL_SECONDS", "30"))
    run_on_startup = _parse_bool(os.getenv("RUN_ON_STARTUP", "false"))
    catchup_on_boot = _parse_bool(os.getenv("SCHEDULE_CATCHUP_ON_BOOT", "false"))

    if poll_seconds < 5:
        poll_seconds = 5

    schedule_time = _parse_schedule_time(schedule_time_raw)
    allowed_days = _parse_schedule_days(schedule_days_raw)
    tz = _get_timezone(timezone_raw)

    logger.info(
        "Scheduler config | TZ=%s | time=%s | days=%s | poll=%ss | run_on_startup=%s | catchup_on_boot=%s",
        timezone_raw,
        schedule_time.strftime("%H:%M"),
        sorted(allowed_days),
        poll_seconds,
        run_on_startup,
        catchup_on_boot,
    )

    if args.print_config:
        return

    city_reset_raw = os.getenv("CITY_ROTATION_RESET_TO", "").strip()
    if city_reset_raw:
        city_name, country = _parse_city_reset_target(city_reset_raw)
        if city_name:
            try:
                db_manager.init_db()
                ok = db_manager.reset_city_rotation(start_city=city_name, start_country=country)
                if ok:
                    if country:
                        logger.info(
                            "City rotation reset applied. Next discovery starts at %s, %s.",
                            city_name,
                            country,
                        )
                    else:
                        logger.info(
                            "City rotation reset applied. Next discovery starts at %s.",
                            city_name,
                        )
                else:
                    logger.warning(
                        "CITY_ROTATION_RESET_TO='%s' not found in configured cities. Skipping reset.",
                        city_reset_raw,
                    )
            except Exception:
                logger.exception("Failed to apply city rotation reset.")

    if args.once:
        _run_pipeline_once("manual --once")
        return

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    startup_ran = False
    last_scheduled_run_date = None

    # Prevent immediate runs after restarts/redeploys once the daily target
    # time has already passed (unless explicitly enabled).
    # When catch-up is disabled, we strictly wait for the next scheduled day.
    now_boot = datetime.now(tz)
    if (
        not catchup_on_boot
        and now_boot.isoweekday() in allowed_days
        and _is_after_schedule(now_boot, schedule_time)
    ):
        last_scheduled_run_date = now_boot.date()
        logger.info(
            "Boot after schedule time (%s). Catch-up disabled; waiting until next scheduled day.",
            schedule_time.strftime("%H:%M"),
        )

    logger.info("Scheduler loop started.")
    while not _shutdown_requested:
        now = datetime.now(tz)

        if run_on_startup and not startup_ran:
            startup_ran = True
            _run_pipeline_once("startup")

        if _should_trigger_scheduled_run(
            now=now,
            schedule_time=schedule_time,
            allowed_days=allowed_days,
            last_scheduled_run_date=last_scheduled_run_date,
        ):
            # Mark the date before running to avoid duplicate triggers if the
            # run takes long and crosses minute boundaries.
            last_scheduled_run_date = now.date()
            _run_pipeline_once("daily schedule")

        time.sleep(poll_seconds)

    logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()

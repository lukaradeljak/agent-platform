"""
Supabase Auth (GoTrue) admin operations.

Used to provision a client user account and generate a password setup/reset link
that we can send from our own Gmail onboarding email.
"""

from __future__ import annotations

import os
import requests

from _helpers import setup_env, setup_logging

setup_env()
log = setup_logging("supabase_auth_ops")


def _supabase_url() -> str:
    url = os.getenv("SUPABASE_URL", "").rstrip("/")
    if not url:
        raise RuntimeError("SUPABASE_URL is not set")
    return url


def _service_role_key() -> str:
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    if not key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY is not set")
    return key


def _admin_headers() -> dict:
    key = _service_role_key()
    # Supabase requires both headers for some endpoints.
    return {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "Content-Type": "application/json",
    }


def generate_auth_link(
    *,
    link_type: str,
    email: str,
    redirect_to: str | None = None,
    data: dict | None = None,
) -> str:
    """
    Generates an auth link (invite/recovery/magiclink) using the Admin API.
    Returns the `action_link`.

    link_type: one of "invite", "recovery", "magiclink", "signup".
    """
    url = f"{_supabase_url()}/auth/v1/admin/generate_link"
    payload: dict = {"type": link_type, "email": email}
    # GoTrue expects `redirect_to` at top-level (defaults to SITE_URL).
    if redirect_to:
        payload["redirect_to"] = redirect_to
    if data:
        payload["data"] = data

    resp = requests.post(url, headers=_admin_headers(), json=payload, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Supabase generate_link failed ({resp.status_code}): {resp.text}")

    action_link = resp.json().get("action_link", "")
    if not action_link:
        raise RuntimeError("Supabase generate_link: missing action_link in response")
    return action_link


def get_password_setup_link(
    *,
    email: str,
    redirect_to: str | None = None,
    data: dict | None = None,
) -> str:
    """
    Returns a link the client can use to set/change their password.

    Strategy:
    - Try `invite` first (good for new users, sets metadata).
    - If the user already exists, fall back to `recovery`.
    """
    try:
        return generate_auth_link(
            link_type="invite",
            email=email,
            redirect_to=redirect_to,
            data=data,
        )
    except Exception as e:
        msg = str(e)
        if "already registered" in msg.lower() or "already exists" in msg.lower():
            log.info("Supabase user already exists, using recovery link")
            return generate_auth_link(
                link_type="recovery",
                email=email,
                redirect_to=redirect_to,
                data=data,
            )
        # If the error is something else (bad creds, etc.), surface it.
        raise


def default_redirect_to() -> str | None:
    """
    Optional redirect target after the user completes the auth flow.
    If not set, Supabase uses the project's Site URL / default settings.
    """
    return os.getenv("APP_PASSWORD_RESET_REDIRECT_URL", "").strip() or None

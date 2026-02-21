"""
Supabase Auth (GoTrue) admin operations.

Used to provision a client user account and generate a password setup/reset link
that we can send from our own Gmail onboarding email.
"""

from __future__ import annotations

import os
import secrets
import string
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


def generate_temp_password(length: int = 12) -> str:
    """Genera una contraseña temporal segura."""
    alphabet = string.ascii_letters + string.digits
    password = [
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.digits),
    ]
    password += [secrets.choice(alphabet) for _ in range(length - 3)]
    secrets.SystemRandom().shuffle(password)
    return "".join(password)


def get_user_id_by_email(email: str) -> str | None:
    """Busca el ID de un usuario por email via Admin API."""
    url = f"{_supabase_url()}/auth/v1/admin/users"
    resp = requests.get(
        url,
        headers=_admin_headers(),
        params={"email": email, "page": 1, "per_page": 1},
        timeout=30,
    )
    if resp.status_code >= 400:
        return None
    users = resp.json().get("users", [])
    return users[0]["id"] if users else None


def generate_magic_link(*, email: str, app_url: str) -> str:
    """
    Genera un magic link que lleva directamente a /auth/confirm → /update-password.
    El usuario hace un click y puede crear su contraseña sin necesidad de recordar
    la contraseña temporal.

    Retorna la URL completa lista para incluir en el email.
    """
    url = f"{_supabase_url()}/auth/v1/admin/generate_link"
    payload = {"type": "magiclink", "email": email}

    resp = requests.post(url, headers=_admin_headers(), json=payload, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(
            f"Supabase generate_link (magiclink) failed ({resp.status_code}): {resp.text}"
        )

    hashed_token = resp.json().get("hashed_token", "")
    if not hashed_token:
        raise RuntimeError("Supabase generate_link: missing hashed_token in response")

    app_base = app_url.rstrip("/")
    return f"{app_base}/auth/confirm?token_hash={hashed_token}&type=magiclink&next=/update-password"


def create_or_update_user_with_password(
    *,
    email: str,
    password: str,
    data: dict | None = None,
) -> str:
    """
    Crea un usuario nuevo con la contraseña dada (email_confirm=True).
    Si el usuario ya existe, actualiza su contraseña.
    Retorna 'created' o 'updated'.
    """
    url = f"{_supabase_url()}/auth/v1/admin/users"
    payload: dict = {"email": email, "password": password, "email_confirm": True}
    if data:
        payload["data"] = data

    resp = requests.post(url, headers=_admin_headers(), json=payload, timeout=30)
    if resp.status_code < 400:
        log.info(f"Usuario creado en Supabase: {email}")
        return "created"

    # Usuario ya existe → actualizar contraseña
    if resp.status_code == 422:
        log.info(f"Usuario ya existe, actualizando contraseña: {email}")
        user_id = get_user_id_by_email(email)
        if not user_id:
            raise RuntimeError(f"No se encontró el usuario {email} para actualizar")
        update_url = f"{_supabase_url()}/auth/v1/admin/users/{user_id}"
        update_resp = requests.put(
            update_url,
            headers=_admin_headers(),
            json={"password": password},
            timeout=30,
        )
        if update_resp.status_code < 400:
            log.info(f"Contraseña actualizada para: {email}")
            return "updated"
        raise RuntimeError(
            f"Error actualizando contraseña ({update_resp.status_code}): {update_resp.text}"
        )

    raise RuntimeError(f"Supabase create user failed ({resp.status_code}): {resp.text}")


def default_redirect_to() -> str | None:
    """
    Optional redirect target after the user completes the auth flow.
    If not set, Supabase uses the project's Site URL / default settings.
    """
    return os.getenv("APP_PASSWORD_RESET_REDIRECT_URL", "").strip() or None

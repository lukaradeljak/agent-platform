"""
Polling script — detecta deals nuevos en "Cierre ganado" y ejecuta onboarding.
Diseñado para correr como Cloud Run Job (cron cada 10 min).

Flujo simplificado (sin PayPal):
  1. Busca deals en Cierre ganado sin onboarding_status
  2. Crea usuario en la app + genera link para crear contraseña
  3. Envía email de bienvenida
  4. Mueve deal a "Cliente Activo"
"""

import sys
sys.path.insert(0, ".")

from _helpers import setup_env, setup_logging
from hubspot_ops import search_deals, get_deal, update_deal, get_deal_contact_email
from gmail_ops import send_onboarding_email
from supabase_auth_ops import create_or_update_user_with_password, generate_temp_password, generate_magic_link
import os

setup_env()
log = setup_logging("poll_hubspot")

STAGE_CIERRE_GANADO = "1297155942"
STAGE_CLIENTE_ACTIVO = "1297805378"


def handle_deal_won(deal_id: str):
    """Procesa un deal que acaba de entrar en Cierre ganado."""
    log.info(f"=== Procesando deal {deal_id} ===")

    deal = get_deal(deal_id)
    client_name = deal.get("dealname", "Cliente")
    company_name = (deal.get("modal_app_name", "") or "").strip() or (client_name or "").strip()
    client_email = deal.get("client_email", "")

    if not client_email:
        client_email = get_deal_contact_email(deal_id) or ""

    if not client_email:
        log.error(f"Deal {deal_id}: sin email de cliente. Marcando como 'new'.")
        update_deal(deal_id, {"onboarding_status": "new"})
        return False

    # 1. Crear usuario en Supabase con contraseña temporal
    temp_password = generate_temp_password()
    app_url = os.getenv("APP_LOGIN_URL", "https://app.acemsystems.com")
    create_or_update_user_with_password(
        email=client_email,
        password=temp_password,
        data={
            "client_name": client_name,
            "hubspot_deal_id": deal_id,
            "must_reset_password": True,
        },
    )

    # Generar magic link directo a /update-password (un solo click)
    try:
        setup_url = generate_magic_link(email=client_email, app_url=app_url)
    except Exception as e:
        log.warning(f"No se pudo generar magic link, el email mostrará solo la contraseña temporal: {e}")
        setup_url = ""

    # 2. Enviar email de bienvenida con magic link + contraseña temporal de respaldo
    send_onboarding_email(
        to=client_email,
        client_name=client_name,
        company_name=company_name,
        client_email=client_email,
        temp_password=temp_password,
        setup_url=setup_url,
    )
    log.info(f"Email de bienvenida enviado a {client_email}")

    # 3. Actualizar deal — mover a "Cliente Activo"
    # Set status first to avoid re-processing loops if stage update fails.
    update_deal(deal_id, {"onboarding_status": "active"})
    try:
        update_deal(deal_id, {"dealstage": STAGE_CLIENTE_ACTIVO})
    except Exception as e:
        log.warning(f"Deal {deal_id}: no se pudo mover a Cliente Activo ({STAGE_CLIENTE_ACTIVO}): {e}")

    log.info(f"=== Deal {deal_id} completado ===")
    return True


def main() -> dict:
    log.info("Buscando deals nuevos en Cierre ganado...")
    stats = {"deals_found": 0, "deals_processed": 0, "emails_sent": 0, "errors_count": 0}

    # Buscar deals en Cierre ganado que NO estén ya procesados
    # (onboarding_status vacío, "new", o inexistente)
    PROCESSED = ["active", "pending_payment", "paused"]
    # Algunos workflows de HubSpot pueden mover el deal inmediatamente a "Cliente Activo".
    # Para no perdernos el onboarding, buscamos en ambas etapas.
    all_won = []
    for stage in (STAGE_CIERRE_GANADO, STAGE_CLIENTE_ACTIVO):
        all_won.extend(
            search_deals(
                filters=[
                    {"propertyName": "dealstage", "operator": "EQ", "value": stage},
                ],
            )
        )
    # De-dup por id
    seen = set()
    uniq = []
    for d in all_won:
        did = str(d.get("id"))
        if did and did not in seen:
            seen.add(did)
            uniq.append(d)
    all_won = uniq
    deals = [d for d in all_won if d.get("onboarding_status") not in PROCESSED]

    if not deals:
        log.info("No hay deals nuevos para procesar.")
        return stats

    stats["deals_found"] = len(deals)
    log.info(f"Encontrados {len(deals)} deals nuevos.")

    for deal in deals:
        deal_id = str(deal["id"])
        try:
            ok = handle_deal_won(deal_id)
            if ok:
                stats["deals_processed"] += 1
                stats["emails_sent"] += 1
        except Exception as e:
            log.error(f"Error procesando deal {deal_id}: {e}")
            stats["errors_count"] += 1

    return stats


if __name__ == "__main__":
    result = main()
    print(f"Stats: {result}")

"""
Operaciones de HubSpot: leer y actualizar deals.
"""

import os
import requests
from _helpers import setup_env, setup_logging

setup_env()
log = setup_logging("hubspot_ops")

BASE_URL = "https://api.hubapi.com"


def _headers() -> dict:
    token = os.getenv("HUBSPOT_ACCESS_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

def get_deal_pipelines() -> dict:
    """Devuelve pipelines/stages de deals (para resolver IDs por label)."""
    url = f"{BASE_URL}/crm/v3/pipelines/deals"
    resp = requests.get(url, headers=_headers())
    resp.raise_for_status()
    return resp.json()


def find_deal_stage_id(label_contains: str, pipeline_id: str = "default") -> str:
    """
    Busca un stage ID por label (case-insensitive) dentro de un pipeline.
    """
    label_contains = (label_contains or "").strip().lower()
    if not label_contains:
        raise ValueError("label_contains is required")

    data = get_deal_pipelines()
    for pipe in data.get("results", []):
        if pipe.get("id") != pipeline_id:
            continue
        for st in pipe.get("stages", []):
            label = (st.get("label") or "").strip().lower()
            if label_contains in label:
                return str(st.get("id"))

    # Helpful error message
    labels = []
    for pipe in data.get("results", []):
        if pipe.get("id") != pipeline_id:
            continue
        for st in pipe.get("stages", []):
            labels.append(f"{st.get('label')} (id={st.get('id')})")
    raise RuntimeError(
        f"No se encontro stage con label que contenga {label_contains!r} en pipeline {pipeline_id}. "
        f"Stages: {', '.join(labels)}"
    )


def search_contacts_by_email(email: str) -> str | None:
    """Devuelve el contact ID si existe un contacto con ese email."""
    email = (email or "").strip()
    if not email:
        raise ValueError("email is required")
    url = f"{BASE_URL}/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": [
            {"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}
        ],
        "properties": ["email"],
        "limit": 1,
    }
    resp = requests.post(url, headers=_headers(), json=payload)
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        return None
    return str(results[0]["id"])


def get_or_create_contact(email: str, firstname: str | None = None, lastname: str | None = None) -> str:
    """Obtiene o crea un contacto por email y devuelve su ID."""
    existing = search_contacts_by_email(email)
    if existing:
        return existing

    props = {"email": email}
    if firstname:
        props["firstname"] = firstname
    if lastname:
        props["lastname"] = lastname

    url = f"{BASE_URL}/crm/v3/objects/contacts"
    resp = requests.post(url, headers=_headers(), json={"properties": props})
    resp.raise_for_status()
    return str(resp.json()["id"])


def create_deal(properties: dict) -> str:
    """Crea un deal y devuelve su ID."""
    url = f"{BASE_URL}/crm/v3/objects/deals"
    resp = requests.post(url, headers=_headers(), json={"properties": properties})
    resp.raise_for_status()
    return str(resp.json()["id"])


def associate_deal_to_contact(deal_id: str, contact_id: str) -> bool:
    """
    Asocia un deal a un contacto.
    Intenta v4 y, si falla, v3.
    """
    deal_id = str(deal_id)
    contact_id = str(contact_id)

    # v4 (newer)
    try:
        url = f"{BASE_URL}/crm/v4/objects/deals/{deal_id}/associations/contacts/{contact_id}/deal_to_contact"
        resp = requests.put(url, headers=_headers())
        if resp.status_code < 400:
            return True
    except Exception:
        pass

    # v3 (fallback)
    try:
        url = f"{BASE_URL}/crm/v3/objects/deals/{deal_id}/associations/contacts/{contact_id}/deal_to_contact"
        resp = requests.put(url, headers=_headers())
        if resp.status_code < 400:
            return True
        log.warning(f"No se pudo asociar deal->contact (v3): {resp.status_code} {resp.text}")
        return False
    except Exception as e:
        log.warning(f"No se pudo asociar deal->contact: {e}")
        return False


def get_deal(deal_id: str, properties: list[str] | None = None) -> dict:
    """
    Obtiene un deal de HubSpot con sus propiedades.
    Retorna dict con las propiedades del deal.
    """
    props = properties or [
        "dealname", "dealstage", "amount",
        "setup_price", "monthly_price", "service_type",
        "modal_app_name", "client_email", "onboarding_status",
        "stripe_customer_id", "stripe_subscription_id",
        "slack_channel_id", "notion_page_id",
    ]

    params = {"properties": ",".join(props)}
    url = f"{BASE_URL}/crm/v3/objects/deals/{deal_id}"

    resp = requests.get(url, headers=_headers(), params=params)
    resp.raise_for_status()

    data = resp.json()
    log.info(f"Deal obtenido: {deal_id} ({data['properties'].get('dealname', '?')})")
    return data["properties"]


def update_deal(deal_id: str, properties: dict) -> bool:
    """
    Actualiza propiedades de un deal en HubSpot.
    properties: dict de {nombre_propiedad: valor}.
    """
    url = f"{BASE_URL}/crm/v3/objects/deals/{deal_id}"
    payload = {"properties": properties}

    resp = requests.patch(url, headers=_headers(), json=payload)
    resp.raise_for_status()

    log.info(f"Deal actualizado: {deal_id} -> {list(properties.keys())}")
    return True


def get_deal_contact_email(deal_id: str) -> str | None:
    """
    Obtiene el email del contacto principal asociado a un deal.
    """
    # Obtener contactos asociados al deal
    url = f"{BASE_URL}/crm/v3/objects/deals/{deal_id}/associations/contacts"
    resp = requests.get(url, headers=_headers())
    resp.raise_for_status()

    results = resp.json().get("results", [])
    if not results:
        log.warning(f"Deal {deal_id} no tiene contactos asociados")
        return None

    # Obtener el email del primer contacto
    contact_id = results[0]["id"]
    url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}"
    resp = requests.get(
        url, headers=_headers(), params={"properties": "email,firstname,lastname"}
    )
    resp.raise_for_status()

    props = resp.json()["properties"]
    email = props.get("email")
    log.info(f"Contacto del deal {deal_id}: {email}")
    return email


def search_deals(filters: list[dict], properties: list[str] | None = None) -> list[dict]:
    """
    Busca deals en HubSpot usando filtros.
    filters: lista de dicts con propertyName, operator, value.
    Retorna lista de dicts con id + properties de cada deal.
    """
    props = properties or [
        "dealname", "dealstage", "amount",
        "setup_price", "monthly_price", "service_type",
        "client_email", "onboarding_status",
    ]
    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": props,
        "limit": 100,
    }
    url = f"{BASE_URL}/crm/v3/objects/deals/search"
    resp = requests.post(url, headers=_headers(), json=payload)
    resp.raise_for_status()

    results = resp.json().get("results", [])
    log.info(f"Busqueda de deals: {len(results)} encontrados")
    return [{"id": r["id"], **r.get("properties", {})} for r in results]


if __name__ == "__main__":
    print("=== Test hubspot_ops ===")
    print(f"HubSpot token configurado: {'sí' if os.getenv('HUBSPOT_ACCESS_TOKEN') else 'no'}")

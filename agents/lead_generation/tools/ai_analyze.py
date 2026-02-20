"""
Stage 3: AI Company Analysis with Gemini (primary) and OpenAI (fallback).
Generates company summaries and 3 tailored automation suggestions per lead.
"""

import json
import logging
import time

import requests

from tools.config import (
    GEMINI_API_KEY,
    GEMINI_API_URL,
    OPENAI_API_KEY,
    OPENAI_API_URL,
    OPENAI_MODEL,
    LEADS_PER_DAY,
    AI_MAX_TOKENS,
    AI_TEMPERATURE,
    AI_REQUEST_DELAY,
)
from tools.db_manager import get_leads_needing_ai, update_lead_ai
from tools.utils import safe_json_parse, retry

logger = logging.getLogger("pipeline")


def _build_prompt(lead):
    """Build the analysis prompt for a lead, in Spanish."""
    company = lead.get("company_name", "Desconocida")
    city = lead.get("city", "")
    country = lead.get("country", "")
    website = lead.get("website", "")
    phone = lead.get("phone", "")
    snippet = lead.get("snippet", "")
    scraped_text = lead.get("scraped_text", "")

    # Combine available info about the company
    company_info = f"Empresa: {company}"
    if city or country:
        company_info += f"\nUbicacion: {city}, {country}"
    if website:
        company_info += f"\nWebsite: {website}"
    if phone:
        company_info += f"\nTelefono: {phone}"
    if snippet:
        company_info += f"\nDescripcion (buscador): {snippet}"
    if scraped_text:
        company_info += f"\nContenido del sitio web: {scraped_text}"

    prompt = f"""Eres un consultor experto en automatizacion de negocios. Analiza la siguiente agencia de marketing digital y sugiere formas en las que podrian beneficiarse de la automatizacion.

{company_info}

Basandote en esta informacion:

1. Escribe un resumen de 2-3 frases sobre que hace esta agencia, que servicios ofrece, y quienes son sus clientes probables.

2. Sugiere exactamente 3 automatizaciones especificas y accionables que esta agencia podria implementar o que les podrias vender. Para cada automatizacion:
   - Nombre conciso
   - Explicacion de que hace en 1-2 frases
   - Valor de negocio concreto (tiempo ahorrado, impacto en ingresos, eficiencia)

Enfocate en automatizaciones practicas y realistas: workflows de CRM, secuencias de email automatizadas, reportes automaticos para clientes, lead scoring, onboarding automatizado de clientes, generacion automatica de propuestas, automatizacion de redes sociales, chatbots, integraciones entre herramientas, dashboards en tiempo real, facturacion automatica, etc.

Las automatizaciones deben ser especificas para esta agencia basandote en sus servicios. NO des sugerencias genericas.

Responde UNICAMENTE con este formato JSON valido, sin texto adicional:
{{
  "summary": "...",
  "automations": [
    {{"name": "...", "description": "...", "value": "..."}},
    {{"name": "...", "description": "...", "value": "..."}},
    {{"name": "...", "description": "...", "value": "..."}}
  ]
}}"""
    return prompt


@retry(max_attempts=2, backoff_factor=3, exceptions=(requests.RequestException,))
def _call_gemini(prompt):
    """Call Gemini API and return the text response."""
    url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": AI_TEMPERATURE,
            "maxOutputTokens": AI_MAX_TOKENS,
            "responseMimeType": "application/json",
        },
    }
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Extract text from Gemini response
    candidates = data.get("candidates", [])
    if candidates:
        parts = candidates[0].get("content", {}).get("parts", [])
        if parts:
            return parts[0].get("text", "")
    return None


@retry(max_attempts=2, backoff_factor=3, exceptions=(requests.RequestException,))
def _call_openai(prompt):
    """Call OpenAI API as fallback and return the text response."""
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": "Eres un consultor experto en automatizacion de negocios. Responde siempre en espanol y en JSON valido."},
            {"role": "user", "content": prompt},
        ],
        "temperature": AI_TEMPERATURE,
        "max_tokens": AI_MAX_TOKENS,
        "response_format": {"type": "json_object"},
    }
    response = requests.post(OPENAI_API_URL, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    choices = data.get("choices", [])
    if choices:
        return choices[0].get("message", {}).get("content", "")
    return None


def _analyze_lead(lead):
    """
    Analyze a single lead using AI. Returns (summary, automations) or (None, None).
    Tries Gemini first, falls back to OpenAI.
    """
    prompt = _build_prompt(lead)

    # Try Gemini first
    if GEMINI_API_KEY:
        try:
            text = _call_gemini(prompt)
            if text:
                parsed = safe_json_parse(text)
                if parsed and "summary" in parsed and "automations" in parsed:
                    return parsed["summary"], parsed["automations"]
                logger.warning("Gemini returned invalid JSON, trying OpenAI...")
        except Exception as e:
            logger.warning(f"Gemini failed: {e}, trying OpenAI...")

    # Fallback to OpenAI
    if OPENAI_API_KEY:
        try:
            text = _call_openai(prompt)
            if text:
                parsed = safe_json_parse(text)
                if parsed and "summary" in parsed and "automations" in parsed:
                    return parsed["summary"], parsed["automations"]
                logger.warning("OpenAI returned invalid JSON")
        except Exception as e:
            logger.warning(f"OpenAI also failed: {e}")

    # Last resort: generic summary
    return _generic_fallback(lead)


def _generic_fallback(lead):
    """Generate a generic summary when both AI APIs fail."""
    company = lead.get("company_name", "Esta agencia")
    city = lead.get("city", "")
    country = lead.get("country", "")
    snippet = lead.get("snippet", "")

    summary = f"{company} es una agencia de marketing digital ubicada en {city}, {country}."
    if snippet:
        summary += f" {snippet[:150]}"

    automations = [
        {
            "name": "Automatizacion de reportes para clientes",
            "description": "Sistema automatico que genera y envia reportes mensuales de rendimiento a cada cliente con metricas de campanas, ROI y recomendaciones.",
            "value": "Ahorra 5-10 horas semanales en generacion manual de reportes y mejora la retencion de clientes.",
        },
        {
            "name": "Secuencias de email para captacion de leads",
            "description": "Flujo automatizado de emails de seguimiento para prospectos que muestran interes, con contenido personalizado segun la industria del prospecto.",
            "value": "Aumenta la tasa de conversion de leads en un 20-30% y libera tiempo del equipo comercial.",
        },
        {
            "name": "Dashboard centralizado en tiempo real",
            "description": "Panel integrado que conecta Google Ads, Meta Ads, Analytics y CRM para visualizar el rendimiento de todas las campanas en un solo lugar.",
            "value": "Reduccion del 70% en tiempo de recopilacion de datos y toma de decisiones mas rapida basada en datos actualizados.",
        },
    ]
    return summary, automations


def run():
    """
    Run AI analysis on all leads that need it.
    Returns count of leads analyzed.
    """
    if not GEMINI_API_KEY and not OPENAI_API_KEY:
        logger.error("No AI API keys configured. Skipping analysis.")
        return 0

    leads = get_leads_needing_ai(limit=LEADS_PER_DAY)
    if not leads:
        logger.info("No leads need AI analysis.")
        return 0

    logger.info(f"AI analysis: processing {len(leads)} leads...")
    analyzed = 0

    for i, lead in enumerate(leads):
        company = lead.get("company_name", "Unknown")
        logger.debug(f"  [{i+1}/{len(leads)}] Analyzing: {company}")

        try:
            summary, automations = _analyze_lead(lead)
            if summary and automations:
                update_lead_ai(lead["id"], summary, automations)
                analyzed += 1
                logger.debug(f"    -> Summary: {summary[:80]}...")
                if isinstance(automations, list):
                    for j, auto in enumerate(automations):
                        name = auto.get("name", "N/A") if isinstance(auto, dict) else str(auto)
                        logger.debug(f"    -> Auto {j+1}: {name}")
        except Exception as e:
            logger.warning(f"    -> Analysis failed for {company}: {e}")

        # Delay between API calls
        time.sleep(AI_REQUEST_DELAY)

    logger.info(f"AI analysis complete: {analyzed}/{len(leads)} leads analyzed")
    return analyzed


if __name__ == "__main__":
    from tools.utils import setup_logging
    from tools.db_manager import init_db

    setup_logging()
    init_db()
    count = run()
    print(f"Analyzed {count} leads with AI")

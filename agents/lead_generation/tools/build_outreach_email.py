"""
AI-Powered Personalized Outreach Email Generator.
Creates highly personalized cold emails based on lead data and AI analysis.
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
    AI_MAX_TOKENS,
    AI_TEMPERATURE,
    AI_REQUEST_DELAY,
)
from tools.utils import safe_json_parse, retry

logger = logging.getLogger("pipeline")


def _build_email_prompt(lead, email_type='initial'):
    """Build the prompt for generating a personalized outreach email."""
    company = lead.get("company_name", "tu empresa")
    contact_name = lead.get("contact_name", "")
    city = lead.get("city", "")
    country = lead.get("country", "")
    ai_summary = lead.get("ai_summary", "")
    automations_raw = lead.get("automation_suggestions", "")

    # Parse automations (all 3)
    automations_text = ""
    automations_brief = ""
    try:
        if isinstance(automations_raw, str):
            automations = json.loads(automations_raw)
        else:
            automations = automations_raw
        if automations and isinstance(automations, list):
            for i, auto in enumerate(automations[:3], 1):
                if isinstance(auto, dict):
                    name = auto.get("name", "")
                    desc = auto.get("description", "")
                    value = auto.get("value", "")
                    automations_text += f"- {name}: {desc} (Beneficio: {value})\n"
                    automations_brief += f"{i}. {name}\n"
    except (json.JSONDecodeError, TypeError):
        pass

    # Determine greeting
    if contact_name:
        first_name = contact_name.split()[0]
        greeting_hint = f"Saludo: Hola {first_name}"
    else:
        greeting_hint = "Saludo: Hola (sin nombre, usar saludo generico profesional)"

    if email_type == 'initial':
        prompt = f"""Eres un experto en copywriting de emails de ventas B2B. Tu trabajo es escribir emails de prospeccion que generen respuestas.

CONTEXTO DEL LEAD:
- Empresa: {company}
- Ubicacion: {city}, {country}
- {greeting_hint}
- Resumen de la empresa: {ai_summary}
- Automatizaciones que les beneficiarian:
{automations_text if automations_text else "- Automatizacion de procesos de marketing y ventas"}

ESTRUCTURA OBLIGATORIA DEL EMAIL (sigue este orden exacto):

1. Saludo: "Hola [nombre],"
2. Observacion de crecimiento: "Vi que en [empresa] estan creciendo en el sector de [su industria/especialidad]. Felicidades por el avance!"
   - Usa el resumen de la empresa para identificar su industria o especialidad concreta (ej: "marketing digital", "publicidad programatica", "branding para startups")
3. Automatizaciones sugeridas: Presenta las 3 automatizaciones de forma natural, breve y fluida. Puedes usar una mini-lista informal o integrarlas en un parrafo corto.
4. Propuesta de valor: "Nosotros ayudamos a empresas como la tuya a automatizar esos flujos repetitivos. Basicamente, conectamos tus herramientas para que trabajen solas."
5. Prueba social: "Recientemente ayudamos a [tipo de empresa similar, ej: 'una agencia de marketing en Madrid'] a ahorrar X horas a la semana automatizando [proceso relevante]."
   - Inventa un caso creible y relevante basado en su tipo de negocio. Varia el numero de horas (10-20) y el proceso segun las automatizaciones sugeridas.
6. CTA: "Que tan abierto estas a explorar este tipo de soluciones?"
7. Firma (siempre exactamente asi):
   Luka Radeljak
   Consultor de Automatizacion
   ACEM Systems

REGLAS DEL SUBJECT:
- Formato: "Ahorren X horas semanales con ACEM Systems"
- X = un numero estimado de horas que ahorrarian (entre 8 y 20, basado en las automatizaciones sugeridas)

REGLAS GENERALES:
- Maximo 150 palabras en el cuerpo
- Tono cercano, directo, profesional
- TODO en espanol
- NO uses frases genericas como "espero que estes bien"
- Si no tienes nombre del contacto, usa solo "Hola,"

Responde UNICAMENTE con este formato JSON:
{{
  "subject": "...",
  "body": "..."
}}

El body debe ser texto plano con saltos de linea (\\n), NO HTML."""

    else:  # followup
        prompt = f"""Eres un experto en emails de seguimiento de ventas B2B.

CONTEXTO:
- Ya enviaste un email inicial a {company} hace 3 dias
- No han respondido
- El email anterior hablaba sobre automatizacion para su agencia de marketing
- {greeting_hint}

TU OBJETIVO:
Escribir un email de seguimiento corto que:
1. NO repita el email anterior
2. Aporte valor adicional (un tip, una estadistica, un caso de uso)
3. Sea aun mas corto que el primero (maximo 80 palabras)
4. Mantenga el tono amigable, no desesperado
5. Termine con una pregunta simple de si/no

REGLAS:
- NO digas "solo queria hacer seguimiento" o "no se si viste mi email"
- Aporta algo nuevo de valor
- Tono casual pero profesional
- Subject que genere curiosidad (maximo 5 palabras)
- TODO en espanol

FIRMA:
Luka

Responde UNICAMENTE con este formato JSON:
{{
  "subject": "...",
  "body": "..."
}}"""

    return prompt


@retry(max_attempts=2, backoff_factor=3, exceptions=(requests.RequestException,))
def _call_gemini(prompt):
    """Call Gemini API."""
    url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.8,  # Slightly higher for creative writing
            "maxOutputTokens": 800,
            "responseMimeType": "application/json",
        },
    }
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()

    candidates = data.get("candidates", [])
    if candidates:
        parts = candidates[0].get("content", {}).get("parts", [])
        if parts:
            return parts[0].get("text", "")
    return None


@retry(max_attempts=2, backoff_factor=3, exceptions=(requests.RequestException,))
def _call_openai(prompt):
    """Call OpenAI API as fallback."""
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": "Eres un experto en copywriting de emails de ventas B2B. Responde siempre en espanol y en JSON valido."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.8,
        "max_tokens": 800,
        "response_format": {"type": "json_object"},
    }
    response = requests.post(OPENAI_API_URL, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    choices = data.get("choices", [])
    if choices:
        return choices[0].get("message", {}).get("content", "")
    return None


def generate_outreach_email(lead, email_type='initial'):
    """
    Generate a personalized outreach email for a lead.

    Args:
        lead: Dict with lead data (company_name, contact_name, ai_summary, automation_suggestions, etc.)
        email_type: 'initial' or 'followup'

    Returns:
        Dict with: subject (str), body (str), html_body (str)
        Or None if generation fails
    """
    prompt = _build_email_prompt(lead, email_type)

    # Try Gemini first
    if GEMINI_API_KEY:
        try:
            text = _call_gemini(prompt)
            if text:
                parsed = safe_json_parse(text)
                if parsed and "subject" in parsed and "body" in parsed:
                    return _format_email(parsed)
                logger.warning("Gemini returned invalid JSON for email, trying OpenAI...")
        except Exception as e:
            logger.warning(f"Gemini failed for email generation: {e}")

    # Fallback to OpenAI
    if OPENAI_API_KEY:
        try:
            text = _call_openai(prompt)
            if text:
                parsed = safe_json_parse(text)
                if parsed and "subject" in parsed and "body" in parsed:
                    return _format_email(parsed)
                logger.warning("OpenAI returned invalid JSON for email")
        except Exception as e:
            logger.warning(f"OpenAI also failed for email generation: {e}")

    # Fallback to template
    return _fallback_email(lead, email_type)


def _format_email(parsed):
    """Format the parsed email into final structure."""
    body = parsed["body"]

    # Convert plain text body to simple HTML
    html_body = body.replace("\n", "<br>\n")
    html_body = f"""<div style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333;">
{html_body}
</div>"""

    return {
        "subject": parsed["subject"],
        "body": body,
        "html_body": html_body,
    }


def _fallback_email(lead, email_type):
    """Generate a template-based email when AI fails."""
    company = lead.get("company_name", "tu agencia")
    contact_name = lead.get("contact_name", "")
    first_name = contact_name.split()[0] if contact_name else ""

    if email_type == 'initial':
        greeting = f"Hola {first_name}," if first_name else "Hola,"
        subject = "Ahorren 15 horas semanales con ACEM Systems"
        body = f"""{greeting}

Vi que en {company} estan creciendo en el sector de marketing digital. ¡Felicidades por el avance!

Algunas automatizaciones que podrian ayudarles: (las que sugirio gemini) reportes automaticos para clientes, secuencias de email para captacion de leads y dashboards centralizados de metricas.

Nosotros ayudamos a empresas como la tuya a automatizar esos flujos repetitivos. Basicamente, conectamos tus herramientas para que trabajen solas.

Recientemente ayudamos a una agencia de marketing similar a ahorrar 15 horas a la semana automatizando sus reportes.

Que tan abierto estas a explorar este tipo de soluciones?

Luka Radeljak
Consultor de Automatizacion
ACEM Systems"""
    else:
        greeting = f"Hola {first_name}," if first_name else "Hola,"
        subject = "Una idea rapida"
        body = f"""{greeting}

Dato curioso: las agencias que automatizan sus reportes retienen un 23% mas de clientes.

La razon? Los clientes reciben updates consistentes sin que el equipo tenga que dedicar horas.

Tienes 15 minutos esta semana para una llamada rapida?

Luka"""

    html_body = body.replace("\n", "<br>\n")
    html_body = f"""<div style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333;">
{html_body}
</div>"""

    return {
        "subject": subject,
        "body": body,
        "html_body": html_body,
    }


if __name__ == "__main__":
    from tools.utils import setup_logging
    setup_logging()

    # Test with sample lead
    sample_lead = {
        "company_name": "Agencia Marketing Digital Madrid",
        "contact_name": "Carlos Rodriguez",
        "city": "Madrid",
        "country": "Espana",
        "ai_summary": "Agencia especializada en SEO, SEM y gestion de redes sociales para pymes.",
        "automation_suggestions": json.dumps([
            {"name": "Reportes automaticos", "description": "Generacion automatica de reportes mensuales para clientes", "value": "Ahorra 8 horas semanales"},
            {"name": "Lead scoring", "description": "Calificacion automatica de prospectos", "value": "Mejora conversion 25%"},
        ]),
    }

    print("=== EMAIL INICIAL ===")
    email = generate_outreach_email(sample_lead, 'initial')
    if email:
        print(f"Subject: {email['subject']}")
        print(f"Body:\n{email['body']}")

    print("\n=== EMAIL FOLLOWUP ===")
    time.sleep(2)
    followup = generate_outreach_email(sample_lead, 'followup')
    if followup:
        print(f"Subject: {followup['subject']}")
        print(f"Body:\n{followup['body']}")

"""
Stage 4b: Email HTML Body Generation.
Creates a clean, readable HTML email with lead summaries and top automation suggestion.
"""

import json
import logging
from datetime import date

logger = logging.getLogger("pipeline")


def _get_top_automation(lead):
    """Get the first automation suggestion name from a lead."""
    raw = lead.get("automation_suggestions", "")
    if not raw:
        return ""
    try:
        if isinstance(raw, str):
            automations = json.loads(raw)
        else:
            automations = raw
        if automations and isinstance(automations[0], dict):
            return automations[0].get("name", "")
    except (json.JSONDecodeError, TypeError, IndexError):
        pass
    return ""


def run(leads, run_date=None):
    """
    Generate an HTML email body summarizing the leads.
    Returns the HTML string.
    """
    if run_date is None:
        run_date = date.today().isoformat()

    count = len(leads)
    leads_with_email = sum(1 for l in leads if l.get("email"))

    logger.info(f"Building email body for {count} leads...")

    # Build lead cards
    lead_cards = ""
    for i, lead in enumerate(leads):
        company = lead.get("company_name", "Sin nombre")
        city = lead.get("city", "")
        country = lead.get("country", "")
        summary = lead.get("ai_summary", "Sin resumen disponible.")
        email = lead.get("email", "No encontrado")
        website = lead.get("website", "")
        top_auto = _get_top_automation(lead)

        location = f"{city}, {country}" if city else country

        website_link = ""
        if website:
            website_link = f'<a href="{website}" style="color:#0563C1;text-decoration:none;font-size:12px;">{website}</a>'

        auto_tag = ""
        if top_auto:
            auto_tag = f"""
            <div style="margin-top:8px;padding:6px 10px;background:#EEF4FF;border-left:3px solid #3B82F6;border-radius:2px;">
                <span style="font-size:11px;color:#1E40AF;">💡 Automatizacion sugerida:</span>
                <span style="font-size:12px;color:#1E3A5F;font-weight:600;">{top_auto}</span>
            </div>"""

        lead_cards += f"""
        <div style="padding:16px 20px;border-bottom:1px solid #E5E7EB;{'background:#FAFBFC;' if i % 2 == 0 else ''}">
            <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                <div>
                    <div style="font-size:15px;font-weight:700;color:#1B3A5C;margin-bottom:4px;">{company}</div>
                    <div style="font-size:12px;color:#6B7280;margin-bottom:6px;">📍 {location} &nbsp;|&nbsp; ✉️ {email}</div>
                </div>
            </div>
            <div style="font-size:13px;color:#374151;line-height:1.5;margin-top:4px;">{summary}</div>
            {website_link}
            {auto_tag}
        </div>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;font-family:'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background:#F3F4F6;">
    <div style="max-width:700px;margin:20px auto;background:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">

        <!-- Header -->
        <div style="background:linear-gradient(135deg,#1B3A5C,#2563EB);padding:24px 28px;color:white;">
            <div style="font-size:22px;font-weight:700;margin-bottom:4px;">📊 Informe Diario de Leads</div>
            <div style="font-size:14px;opacity:0.9;">{run_date} &nbsp;|&nbsp; {count} agencias de marketing digital</div>
        </div>

        <!-- Stats bar -->
        <div style="display:flex;background:#F0F4FF;padding:12px 28px;border-bottom:1px solid #E5E7EB;">
            <div style="flex:1;text-align:center;">
                <div style="font-size:20px;font-weight:700;color:#1B3A5C;">{count}</div>
                <div style="font-size:11px;color:#6B7280;">Leads totales</div>
            </div>
            <div style="flex:1;text-align:center;">
                <div style="font-size:20px;font-weight:700;color:#059669;">{leads_with_email}</div>
                <div style="font-size:11px;color:#6B7280;">Con email</div>
            </div>
            <div style="flex:1;text-align:center;">
                <div style="font-size:20px;font-weight:700;color:#2563EB;">{count * 3}</div>
                <div style="font-size:11px;color:#6B7280;">Automatizaciones</div>
            </div>
        </div>

        <!-- Lead cards -->
        <div>
            {lead_cards}
        </div>

        <!-- Footer -->
        <div style="padding:20px 28px;background:#F9FAFB;border-top:1px solid #E5E7EB;">
            <div style="font-size:13px;color:#6B7280;text-align:center;">
                📎 Detalles completos con las 3 automatizaciones por empresa en el <strong>Excel adjunto</strong>.
            </div>
            <div style="font-size:11px;color:#9CA3AF;text-align:center;margin-top:8px;">
                Generado automaticamente por el Pipeline de Lead Generation & Enrichment
            </div>
        </div>

    </div>
</body>
</html>"""

    logger.info("Email body generated.")
    return html


if __name__ == "__main__":
    import json
    from tools.config import TMP_DIR

    # Test with sample data
    sample = [
        {
            "company_name": "Agencia Demo",
            "city": "Madrid",
            "country": "Espana",
            "email": "info@demo.com",
            "website": "https://demo.com",
            "ai_summary": "Agencia especializada en marketing digital y SEO para pymes en Espana.",
            "automation_suggestions": json.dumps([
                {"name": "Reportes automaticos", "description": "...", "value": "..."},
            ]),
        }
    ]
    html = run(sample)
    # Save for preview
    TMP_DIR.mkdir(exist_ok=True)
    with open(str(TMP_DIR / "email_preview.html"), "w", encoding="utf-8") as f:
        f.write(html)
    print("Preview saved to .tmp/email_preview.html")

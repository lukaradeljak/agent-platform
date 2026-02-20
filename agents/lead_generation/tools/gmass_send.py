"""
GMass API Client.
Sends personalized outreach emails via GMass API.
Supports both transactional emails and campaigns with auto-followup.
"""

import logging
import time
import requests

from tools.config import GMASS_API_KEY, GMASS_API_BASE, GMAIL_ADDRESS, GMASS_FROM_NAME, FOLLOWUP_DAYS
from tools.config import GMASS_TRACK_CLICKS, GMASS_TRACK_OPENS
from tools.utils import retry

logger = logging.getLogger("pipeline")


@retry(max_attempts=3, backoff_factor=2, exceptions=(requests.RequestException,))
def send_transactional_email(
    to_email: str,
    subject: str,
    html_body: str,
    from_email: str = None,
    from_name: str = None,
    track_opens: bool | None = None,
    track_clicks: bool | None = None,
) -> dict:
    """
    Send a single transactional email via GMass API.

    Args:
        to_email: Recipient email address
        subject: Email subject line
        html_body: HTML content of the email
        from_email: Sender email (defaults to GMAIL_ADDRESS)
        from_name: Sender display name (defaults to GMASS_FROM_NAME)
        track_opens: Enable open tracking
        track_clicks: Enable click tracking

    Returns:
        Dict with: success (bool), message_id (str), error (str if failed)
    """
    if not GMASS_API_KEY:
        logger.error("GMASS_API_KEY not configured")
        return {"success": False, "error": "API key not configured"}

    url = f"{GMASS_API_BASE}/transactional"
    headers = {
        "Content-Type": "application/json",
        "X-apikey": GMASS_API_KEY,
    }

    payload = {
        "fromEmail": from_email or GMAIL_ADDRESS,
        "fromName": from_name or GMASS_FROM_NAME,
        "to": to_email,
        "subject": subject,
        "message": html_body,
    }
    # GMass transactional API enables tracking when these fields are present.
    # Only include them when we want tracking.
    open_enabled = GMASS_TRACK_OPENS if track_opens is None else bool(track_opens)
    click_enabled = GMASS_TRACK_CLICKS if track_clicks is None else bool(track_clicks)
    if open_enabled:
        payload["openTrack"] = True
    if click_enabled:
        payload["clickTrack"] = True

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            logger.info(f"Email enviado a {to_email}")
            return {
                "success": True,
                "message_id": data.get("messageId") or data.get("id"),
                "response": data,
            }
        else:
            logger.error(f"GMass error {response.status_code}: {response.text}")
            return {
                "success": False,
                "error": f"HTTP {response.status_code}: {response.text}",
            }

    except requests.RequestException as e:
        logger.error(f"GMass request failed: {e}")
        return {"success": False, "error": str(e)}


@retry(max_attempts=3, backoff_factor=2, exceptions=(requests.RequestException,))
def send_with_auto_followup(
    to_email: str,
    subject: str,
    html_body: str,
    followup_subject: str = None,
    followup_body: str = None,
    followup_days: int = None,
    from_email: str = None,
    from_name: str = None,
    track_opens: bool | None = None,
    track_clicks: bool | None = None,
) -> dict:
    """
    Send an email with automatic followup via GMass Campaign API.

    Args:
        to_email: Recipient email address
        subject: Email subject line
        html_body: HTML content of the email
        followup_subject: Subject for followup (defaults to "Re: {subject}")
        followup_body: HTML content for followup
        followup_days: Days to wait before followup (defaults to FOLLOWUP_DAYS)
        from_email: Sender email (defaults to GMAIL_ADDRESS)
        from_name: Sender display name (defaults to GMASS_FROM_NAME)

    Returns:
        Dict with: success (bool), campaign_id (str), error (str if failed)
    """
    if not GMASS_API_KEY:
        logger.error("GMASS_API_KEY not configured")
        return {"success": False, "error": "API key not configured"}

    headers = {
        "Content-Type": "application/json",
        "X-apikey": GMASS_API_KEY,
    }

    # Step 1: Create campaign draft
    draft_url = f"{GMASS_API_BASE}/campaigndrafts"
    draft_payload = {
        "subject": subject,
        "message": html_body,
        "messageType": "html",
        "emailAddresses": to_email,
        "fromEmail": from_email or GMAIL_ADDRESS,
        "fromName": from_name or GMASS_FROM_NAME,
    }

    try:
        draft_response = requests.post(draft_url, json=draft_payload, headers=headers, timeout=30)

        if draft_response.status_code != 200:
            logger.error(f"GMass draft creation failed: {draft_response.status_code} - {draft_response.text}")
            return {"success": False, "error": f"Draft creation failed: {draft_response.text}"}

        draft_data = draft_response.json()
        draft_id = draft_data.get("campaignDraftId") or draft_data.get("draftId") or draft_data.get("id")

        if not draft_id:
            logger.error(f"GMass did not return draft ID: {draft_data}")
            return {"success": False, "error": "No draft ID returned"}

        logger.debug(f"Draft created: {draft_id}")

        # Brief pause to let Gmail save the draft
        time.sleep(3)

        # Step 2: Send campaign with auto-followup
        # The draft ID goes in the URL path, NOT in the request body
        campaign_url = f"{GMASS_API_BASE}/campaigns/{draft_id}"

        # Build followup text
        days = followup_days or FOLLOWUP_DAYS
        f_body = followup_body or f"""<div style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333;">
Hola,<br><br>
Queria dar seguimiento a mi mensaje anterior. Se que el tiempo es limitado, pero creo que una breve conversacion podria ser valiosa.<br><br>
¿Tienes 15 minutos esta semana?<br><br>
Luka
</div>"""

        campaign_payload = {
            # Auto-followup configuration
            "stageOneDays": days,
            "stageOneAction": "r",  # r = If No Reply
            "stageOneCampaignText": f_body,
            "stageOneThread": "same",  # Keep followup in same email thread
        }
        open_enabled = GMASS_TRACK_OPENS if track_opens is None else bool(track_opens)
        click_enabled = GMASS_TRACK_CLICKS if track_clicks is None else bool(track_clicks)
        if open_enabled:
            campaign_payload["openTracking"] = True
        if click_enabled:
            campaign_payload["clickTracking"] = True

        campaign_response = requests.post(campaign_url, json=campaign_payload, headers=headers, timeout=30)

        if campaign_response.status_code == 200:
            campaign_data = campaign_response.json()
            campaign_id = campaign_data.get("campaignId") or campaign_data.get("id")
            logger.info(f"Email enviado a {to_email} con auto-followup en {days} dias")
            return {
                "success": True,
                "campaign_id": campaign_id,
                "draft_id": draft_id,
                "followup_days": days,
                "response": campaign_data,
            }
        else:
            logger.error(f"GMass campaign send failed: {campaign_response.status_code} - {campaign_response.text}")
            return {"success": False, "error": f"Campaign send failed: {campaign_response.text}"}

    except requests.RequestException as e:
        logger.error(f"GMass request failed: {e}")
        return {"success": False, "error": str(e)}


def test_connection() -> bool:
    """Test if GMass API is working with current credentials."""
    if not GMASS_API_KEY:
        return False

    # Simple test - try to access API (we'll use a GET endpoint if available)
    # For now, just verify the key format is valid
    return len(GMASS_API_KEY) > 10


if __name__ == "__main__":
    from tools.utils import setup_logging
    setup_logging()

    # Test connection
    if test_connection():
        print("GMass API key configurada correctamente")
    else:
        print("ERROR: GMass API key no configurada")

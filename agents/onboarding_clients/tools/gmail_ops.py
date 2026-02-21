"""
Operaciones de Gmail: envío de emails de onboarding vía SMTP.
Incluye PDF de bienvenida brandeado como adjunto.
"""

import os
import smtplib
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from _helpers import setup_env, setup_logging

setup_env()
log = setup_logging("gmail_ops")

GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
APP_LOGIN_URL = os.getenv("APP_LOGIN_URL", "").strip()
PDF_PATH = Path(__file__).parent / "welcome_template.pdf"

EMAIL_TEMPLATE_VERSION = "2026-02-18.5"
SMTP_TIMEOUT_SECONDS = int(os.getenv("SMTP_TIMEOUT_SECONDS", "20") or "20")
EMAIL_FORCE_PAYPAL_SECTION = os.getenv("EMAIL_FORCE_PAYPAL_SECTION", "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
EMAIL_PAYPAL_PREVIEW_URL = os.getenv("EMAIL_PAYPAL_PREVIEW_URL", "").strip()


def _send_via_smtp(msg: MIMEMultipart) -> None:
    """
    Send message using Gmail SMTP.
    Tries SMTPS (465) first, then SMTP+STARTTLS (587).
    """
    last_err: Exception | None = None

    # 1) SMTPS
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=SMTP_TIMEOUT_SECONDS) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        log.info("Email enviado via SMTPS:465")
        return
    except Exception as e:
        last_err = e
        log.warning(f"Fallo envio via SMTPS:465 ({type(e).__name__}): {e}")

    # 2) STARTTLS
    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=SMTP_TIMEOUT_SECONDS) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        log.info("Email enviado via SMTP STARTTLS:587")
        return
    except Exception as e:
        last_err = e
        raise RuntimeError(f"No se pudo enviar email via SMTP (465/587): {e}") from last_err


def _build_onboarding_html(client_name: str, client_email: str = "", temp_password: str = "", setup_url: str = "") -> str:
    """Genera el HTML del email de onboarding alineado al brand ACEM."""
    return f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin: 0; padding: 0; background-color: #0e1320;">
<table width="100%" cellpadding="0" cellspacing="0" style="background-color: #0e1320;">
<tr><td align="center" style="padding: 40px 20px;">
<table width="600" cellpadding="0" cellspacing="0" style="background-color: #121A2E; border-radius: 12px;">

  <!-- Header -->
  <tr><td style="padding: 40px 40px 20px; text-align: center;">
    <h1 style="font-family: 'Manrope', Arial, sans-serif; color: #E8EDF5; font-size: 24px; margin: 0;">
      Bienvenido a ACEM Systems
    </h1>
    <div style="width: 80px; height: 2px; background: linear-gradient(to right, #00E5FF, #6F00E5); margin: 16px auto 0;"></div>
  </td></tr>

  <!-- Saludo -->
  <tr><td style="padding: 20px 40px 10px; font-family: 'Inter', Arial, sans-serif; color: #8899B0; font-size: 15px; line-height: 1.7;">
    <p>Hola <strong style="color: #E8EDF5;">{client_name}</strong>,</p>
    <p>Estamos emocionados de comenzar a trabajar juntos. A continuaci&oacute;n
    te compartimos __INTRO_STEPS__ para comenzar con tu proyecto.</p>
  </td></tr>

__PAYMENT_SECTION__

  <!-- Paso N: Acceso -->
  <tr><td style="padding: 10px 40px;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #182033; border-radius: 8px;">
      <tr><td style="padding: 20px 24px;">
        <p style="font-family: 'Manrope', Arial, sans-serif; color: #00E5FF; font-size: 13px; font-weight: bold; margin: 0 0 8px; letter-spacing: 0.5px;">
          PASO __PASSWORD_STEP_NUM__ &mdash; CREA TU CONTRASE&Ntilde;A
        </p>
        <p style="font-family: 'Inter', Arial, sans-serif; color: #8899B0; font-size: 14px; line-height: 1.6; margin: 0 0 16px;">
          Hac&eacute; click en el bot&oacute;n para ingresar a tu panel y crear tu contrase&ntilde;a.
          El link es v&aacute;lido por 24 horas.
        </p>
        <table cellpadding="0" cellspacing="0"><tr><td style="background-color: #00E5FF; border-radius: 6px;">
          <a href="__SETUP_URL__"
             style="display: inline-block; color: #121A2E; padding: 12px 32px;
                    text-decoration: none; font-family: 'Manrope', Arial, sans-serif;
                    font-size: 14px; font-weight: bold; letter-spacing: 0.5px;">
            Crear contrase&ntilde;a
          </a>
        </td></tr></table>
        <p style="font-family: 'Inter', Arial, sans-serif; color: #5a6a80; font-size: 12px; line-height: 1.5; margin: 16px 0 4px;">
          &iquest;El link expir&oacute;? Ingres&aacute; con tu contrase&ntilde;a temporal:
        </p>
        <p style="font-family: 'Inter', Arial, sans-serif; color: #8899B0; font-size: 13px; line-height: 1.6; margin: 0 0 2px;">
          <strong style="color: #E8EDF5;">Email:</strong> {client_email}
        </p>
        <p style="font-family: 'Inter', Arial, sans-serif; color: #8899B0; font-size: 13px; line-height: 1.6; margin: 0;">
          <strong style="color: #E8EDF5;">Contrase&ntilde;a temporal:</strong>&nbsp;
          <code style="background: #0e1320; color: #00E5FF; padding: 2px 8px; border-radius: 4px; font-size: 13px; letter-spacing: 1px;">{temp_password}</code>
        </p>
      </td></tr>
    </table>
  </td></tr>

  <!-- Info adicional -->
  <tr><td style="padding: 16px 40px 30px; font-family: 'Inter', Arial, sans-serif; color: #8899B0; font-size: 14px; line-height: 1.6;">
    <p>Nuestro horario de atenci&oacute;n es de <strong style="color: #E8EDF5;">lunes a viernes,
    09:00 a 20:00 hs</strong>. Puedes escribirnos fuera de horario y
    responderemos a primera hora.</p>
    <p style="font-size: 13px; color: #5a6a80;">
      Adjuntamos un PDF con toda la informaci&oacute;n sobre tu incorporaci&oacute;n:
      soporte, garant&iacute;a de satisfacci&oacute;n y t&eacute;rminos del servicio.</p>
  </td></tr>

  <!-- Footer -->
  <tr><td style="padding: 20px 40px; border-top: 1px solid #1e2a3e; text-align: center;">
    <p style="font-family: 'Inter', Arial, sans-serif; color: #4a5a70; font-size: 12px; margin: 0;">
      ACEM Systems &mdash; Efficient autonomous agents for scalable growth.
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def send_onboarding_email(
    to: str,
    client_name: str,
    company_name: str | None = None,
    payment_url: str = "",
    client_email: str = "",
    temp_password: str = "",
    setup_url: str = "",
) -> bool:
    """
    Envía el email de onboarding con enlace de pago y PDF adjunto.
    Retorna True si se envió correctamente.
    """
    msg = MIMEMultipart("mixed")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = to
    subject_company = (company_name or "").strip() or (client_name or "").strip()
    msg["Subject"] = f"Bienvenido a ACEM Systems, {subject_company}"

    # HTML body
    html_part = MIMEMultipart("alternative")
    html = _build_onboarding_html(client_name, client_email=client_email, temp_password=temp_password, setup_url=setup_url)

    show_payment = bool(payment_url) or EMAIL_FORCE_PAYPAL_SECTION
    intro_steps = "los dos pasos" if show_payment else "el paso"
    password_step_num = "2" if show_payment else "1"

    html = html.replace("__INTRO_STEPS__", intro_steps)
    html = html.replace("__PASSWORD_STEP_NUM__", password_step_num)
    html = html.replace("__SETUP_URL__", setup_url or APP_LOGIN_URL or "#")

    if show_payment:
        effective_payment_url = payment_url or EMAIL_PAYPAL_PREVIEW_URL or "#"
        is_preview_payment = (not payment_url) and bool(EMAIL_FORCE_PAYPAL_SECTION)
        button_label = "Pagar Setup" if payment_url else "Pagar Setup (preview)"
        preview_note = (
            "<p style=\"font-family: 'Inter', Arial, sans-serif; color: #5a6a80; font-size: 12px; "
            "line-height: 1.5; margin: 12px 0 0;\">"
            "Este paso est&aacute; en modo preview (solo visual). El link real de pago se activar&aacute; "
            "cuando habilitemos PayPal."
            "</p>"
            if is_preview_payment
            else ""
        )
        payment_section = f"""\
  <!-- Paso 1: Pago -->
  <tr><td style="padding: 10px 40px;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #182033; border-radius: 8px;">
      <tr><td style="padding: 20px 24px;">
        <p style="font-family: 'Manrope', Arial, sans-serif; color: #00E5FF; font-size: 13px; font-weight: bold; margin: 0 0 8px; letter-spacing: 0.5px;">
          PASO 1 &mdash; PAGO DEL SETUP
        </p>
        <p style="font-family: 'Inter', Arial, sans-serif; color: #8899B0; font-size: 14px; line-height: 1.6; margin: 0 0 16px;">
          Para iniciar el desarrollo de tu agente aut&oacute;nomo, realiza el pago
          del setup a trav&eacute;s de PayPal. Una vez confirmado, comenzamos de inmediato.
        </p>
        <table cellpadding="0" cellspacing="0"><tr><td style="background-color: #00E5FF; border-radius: 6px;">
          <a href="{effective_payment_url}"
             style="display: inline-block; color: #121A2E; padding: 12px 32px;
                    text-decoration: none; font-family: 'Manrope', Arial, sans-serif;
                    font-size: 14px; font-weight: bold; letter-spacing: 0.5px;">
            {button_label}
          </a>
        </td></tr></table>
        {preview_note}
      </td></tr>
    </table>
  </td></tr>
"""
    else:
        payment_section = ""

    html = html.replace("__PAYMENT_SECTION__", payment_section)

    # Keep a non-user-facing marker for debugging.
    html = html.replace("</body>", f"<!-- template={EMAIL_TEMPLATE_VERSION} -->\n</body>")
    html_part.attach(MIMEText(html, "html"))
    msg.attach(html_part)

    # PDF attachment
    if PDF_PATH.exists():
        with open(PDF_PATH, "rb") as f:
            pdf = MIMEApplication(f.read(), _subtype="pdf")
            pdf.add_header(
                "Content-Disposition", "attachment",
                filename=f"Bienvenida ACEM Systems - {client_name}.pdf",
            )
            msg.attach(pdf)
        log.info("PDF de bienvenida adjuntado")
    else:
        log.warning(f"PDF no encontrado en {PDF_PATH}")

    try:
        _send_via_smtp(msg)
        log.info(f"Email de onboarding enviado a {to}")
        return True
    except Exception as e:
        log.error(f"Error enviando email a {to}: {e}")
        return False


if __name__ == "__main__":
    print("=== Test gmail_ops ===")
    print(f"Gmail configurado: {'sí' if GMAIL_ADDRESS else 'no'}")
    print(f"PDF existe: {'sí' if PDF_PATH.exists() else 'no'}")
    html = _build_onboarding_html("Test Client", client_email="test@example.com", temp_password="TempPass123", setup_url="https://app.acemsystems.com/auth/confirm?token_hash=test&type=magiclink&next=/update-password")
    html = html.replace("__INTRO_STEPS__", "los dos pasos")
    html = html.replace("__PASSWORD_STEP_NUM__", "2")
    html = html.replace("__PAYMENT_SECTION__", "<!-- payment section -->")
    html = html.replace("__SETUP_URL__", "https://app.acemsystems.com/auth/confirm?token_hash=test&type=magiclink&next=/update-password")
    print(f"HTML generado: {len(html)} chars")

"""
app/utils/email_utils.py

Email / SMS notification helpers (stub implementation).

Wire up a real provider (SendGrid, AWS SES, Twilio, …) by filling in the
bodies of the functions below.  All call-sites in routes / services remain
unchanged because they only call the public functions defined here.

Environment variables needed once enabled:
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS   → for email via SMTP
    TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, ...   → for SMS via Twilio
"""

from typing import List, Optional

from app.core.logger import get_logger

logger = get_logger(__name__)


# ── Email ─────────────────────────────────────────────────────────────────────

def send_email(
    to: List[str],
    subject: str,
    body: str,
    html_body: Optional[str] = None,
) -> bool:
    """
    Send an email to one or more recipients.

    Returns True on success, False on failure.

    Example::

        send_email(
            to=["dispatch@company.co.za"],
            subject="Manifest M-0042 dispatched",
            body="Truck ABC-123 left at 08:30 with 45 invoices.",
        )
    """
    # ── Uncomment and configure once an SMTP / API provider is chosen ──
    # import smtplib
    # from email.mime.text import MIMEText
    # from app.core.config import settings
    # ...
    logger.info(f"[EMAIL STUB] To={to} | Subject={subject!r}")
    return True   # change to False / raise on real errors


# ── SMS ───────────────────────────────────────────────────────────────────────

def send_sms(to: str, message: str) -> bool:
    """
    Send a text message to a single phone number.

    Returns True on success, False on failure.

    Example::

        send_sms("+27821234567", "Your manifest M-0042 has been dispatched.")
    """
    # ── Uncomment and configure once Twilio (or similar) is set up ──
    # from twilio.rest import Client
    # from app.core.config import settings
    # client = Client(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN)
    # client.messages.create(body=message, from_=settings.TWILIO_FROM, to=to)
    logger.info(f"[SMS STUB] To={to} | Message={message!r}")
    return True


# ── Convenience wrappers ──────────────────────────────────────────────────────

def notify_manifest_dispatched(manifest_number: str, driver: str, recipients: List[str]) -> None:
    """Fire-and-forget notification when a manifest is dispatched."""
    subject = f"Manifest {manifest_number} dispatched"
    body    = f"Driver {driver} has dispatched manifest {manifest_number}."
    send_email(recipients, subject, body)


def notify_invoice_exception(invoice_number: str, reason: str, recipients: List[str]) -> None:
    """Alert operations team about a processing exception on an invoice."""
    subject = f"Invoice exception: {invoice_number}"
    body    = f"Invoice {invoice_number} requires attention.\n\nReason: {reason}"
    send_email(recipients, subject, body)

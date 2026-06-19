"""
app/core/constants.py

Single source of truth for all business-domain constants used across the application.
Import from here rather than repeating bare string literals in multiple files.
"""

# ── Order / invoice lifecycle ──────────────────────────────────────────────

# Values stored in orders.type
ORDER_TYPES = ("INVOICE", "CREDIT_NOTE")

# Named shortcuts — use in Python code and as SQL bind parameters
ORDER_TYPE_INVOICE     = "INVOICE"
ORDER_TYPE_CREDIT_NOTE = "CREDIT_NOTE"

# Values stored in orders.status
INVOICE_STATUSES = ("PENDING", "CANCELLED", "PROCESSED", "ORPHAN")

# Named shortcuts — use in Python code and as SQL bind parameters
INVOICE_STATUS_PENDING   = "PENDING"
INVOICE_STATUS_CANCELLED = "CANCELLED"
INVOICE_STATUS_PROCESSED = "PROCESSED"
INVOICE_STATUS_ORPHAN    = "ORPHAN"

# ── Delivery ───────────────────────────────────────────────────────────────

# Values stored in orders.delivery_mode and customer_routes.delivery_mode
DELIVERY_MODES = ("INTERNAL", "THIRD_PARTY")

# Values stored in delivery_events.event_type
DELIVERY_EVENT_TYPES = ("STATUS_CHANGE", "POD_UPLOAD")

# Named shortcuts for direct usage in code (kept in sync with tuples above)
DELIVERY_EVENT_STATUS_CHANGE = "STATUS_CHANGE"
DELIVERY_EVENT_POD_UPLOAD    = "POD_UPLOAD"

# ── Currency ─────────────────────────────────────────────────────────────────

# Values stored in orders.currency and report_items.currency
VALID_CURRENCIES = ("USD", "ZWL")
DEFAULT_CURRENCY = "USD"

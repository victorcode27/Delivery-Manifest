"""
app/services/delivery_service.py

Business-rule helpers for the delivery execution domain.
Extracted from delivery.py — these are service-layer concerns that do not
belong in the route file.

Note: is_manifest_assigned_to_driver receives the route-owned db session as a
parameter and must not open, commit, or close it.
"""

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.schemas.delivery import ALLOWED_TRANSITIONS

logger = get_logger(__name__)

# Statuses from which a transition to DELIVERED is valid under the canonical state machine.
# Derived from ALLOWED_TRANSITIONS so this stays in sync automatically.
# Under current rules this resolves to: frozenset({"IN_TRANSIT"})
BULK_CONFIRMABLE = frozenset(
    s for s, allowed in ALLOWED_TRANSITIONS.items() if "DELIVERED" in allowed
)


def derive_manifest_status(statuses: list) -> str:
    """
    Derive a manifest-level summary status from the list of per-invoice delivery statuses.

    Rules (evaluated in order):
      1. Empty list OR all PENDING                                 → PENDING
      2. All DELIVERED or RETURNED                                 → COMPLETED
      3. All resolved (no PENDING/IN_TRANSIT) + any FAILED/PARTIAL → COMPLETED_WITH_ISSUES
      4. Anything else                                             → IN_PROGRESS
    """
    if not statuses or all(s == "PENDING" for s in statuses):
        return "PENDING"
    if all(s in ("DELIVERED", "RETURNED") for s in statuses):
        return "COMPLETED"
    if all(s in ("DELIVERED", "RETURNED", "FAILED", "PARTIAL") for s in statuses):
        return "COMPLETED_WITH_ISSUES"
    return "IN_PROGRESS"


def is_manifest_assigned_to_driver(
    db: Session, manifest_number: str, user: dict
) -> bool:
    """
    Return True if the manifest is assigned to the given DRIVER-role user,
    either as driver or as assistant.

    Checks both FK columns (driver_user_id, assistant_user_id) and the
    legacy text columns (driver, assistant) so the feature works whether or
    not user IDs have been written to the reports row.

    The caller owns the db session — this function must not commit or close it.
    """
    result = db.execute(
        text("""
            SELECT 1
            FROM   reports
            WHERE  manifest_number = :mn
              AND  (
                       driver_user_id    = :uid OR driver    = :uname
                    OR assistant_user_id = :uid OR assistant = :uname
                   )
            LIMIT  1
        """),
        {"mn": manifest_number, "uid": user["id"], "uname": user["username"]},
    )
    return result.fetchone() is not None


def validate_transition(current_status: str, next_status: str) -> None:
    """
    Raise HTTP 422 if current_status -> next_status is not permitted by the
    canonical ALLOWED_TRANSITIONS map defined in schemas.delivery.

    Called before every status UPSERT to enforce the backend state machine.
    The frontend may mirror these rules for UX, but this check is authoritative.
    """
    allowed = ALLOWED_TRANSITIONS.get(current_status, frozenset())
    if next_status not in allowed:
        if not allowed:
            detail = (
                f"Cannot update delivery status: '{current_status}' is a terminal "
                f"status and accepts no further transitions."
            )
        else:
            detail = (
                f"Invalid delivery status transition: '{current_status}' -> '{next_status}'. "
                f"Allowed next statuses from '{current_status}': "
                f"{', '.join(sorted(allowed))}."
            )
        raise HTTPException(status_code=422, detail=detail)


def fetch_report_item(db: Session, report_item_id: int):
    """
    Look up a report_item row joined to its parent report.

    Returns the raw row with attributes: id, invoice_number, manifest_number,
    driver, driver_user_id, assistant, assistant_user_id.

    Raises HTTP 404 if no row exists for the given report_item_id.
    The caller owns the db session — this function must not commit or close it.
    """
    item_row = db.execute(
        text("""
            SELECT ri.id, ri.invoice_number, r.manifest_number,
                   r.driver, r.driver_user_id,
                   r.assistant, r.assistant_user_id
            FROM   report_items ri
            JOIN   reports       r ON r.id = ri.report_id
            WHERE  ri.id = :id
        """),
        {"id": report_item_id},
    ).fetchone()
    if not item_row:
        raise HTTPException(status_code=404, detail="Report item not found")
    return item_row


def assert_driver_item_access(item_row, current_user: dict) -> None:
    """
    Raise HTTP 403 if the current user is a DRIVER and the item does not
    belong to a manifest assigned to them (as driver or assistant).

    Non-DRIVER roles pass through without any check.
    The caller owns the db session — this function does not touch the database.
    """
    if current_user.get("role") == "DRIVER":
        assigned = (
            item_row.driver_user_id    == current_user["id"]
            or item_row.driver         == current_user["username"]
            or item_row.assistant_user_id == current_user["id"]
            or item_row.assistant      == current_user["username"]
        )
        if not assigned:
            raise HTTPException(
                status_code=403,
                detail="Access denied: item not in your manifest",
            )

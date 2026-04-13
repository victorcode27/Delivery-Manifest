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

# ── Analytics: canonical driver identity grouping rule ────────────────────────
#
# When grouping or attributing deliveries by driver in analytics queries, apply
# this precedence to avoid double-counting and handle legacy text-only records:
#
#   1. PRIMARY:  reports.driver_user_id  (FK to users.id — preferred, exact match)
#   2. FALLBACK: reports.driver          (TEXT — used when driver_user_id IS NULL)
#   3. UNKNOWN:  label as "Unassigned"   (when both driver_user_id IS NULL
#                                         AND driver IS NULL / blank / 'N/A')
#
# Do NOT use delivery_updates.driver_user_id as the grouping key for analytics.
# That column reflects who last updated the delivery status, not who was assigned
# the manifest.  The manifest-level assignment lives only in reports.*
#
# This rule applies to:
#   GET /api/analytics/drivers   — per-driver performance breakdown
#   GET /api/analytics/manifests — driver name displayed per manifest
#   GET /api/analytics/overview  — driver filter param resolution
# ─────────────────────────────────────────────────────────────────────────────


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


def bulk_update_manifest_items(
    db: Session,
    manifest_number: str,
    target_status: str,
    current_user: dict,
) -> dict:
    """
    Bulk-update all eligible invoices in a manifest to target_status.

    Eligibility is derived dynamically from ALLOWED_TRANSITIONS — only items
    whose current status lists target_status as a valid next step are updated.
    PENDING items are never auto-advanced through intermediate states.

    For each eligible item writes:
      - one delivery_updates UPSERT (preserves existing notes and driver linkage)
      - one delivery_events audit row

    All writes run inside the caller's transaction; the caller is responsible
    for commit and rollback.

    Returns {"updated": int, "skipped": int}.
    """
    from delivery_manifest_backend.app.core.constants import DELIVERY_EVENT_STATUS_CHANGE

    # Statuses from which target_status is a valid next step per the state machine.
    #
    # Special case — bulk IN_TRANSIT is a dispatch action, not a reopen/retry action.
    # Although ALLOWED_TRANSITIONS permits FAILED, PARTIAL, and RETURNED to re-enter
    # IN_TRANSIT, the bulk operation must only move PENDING items (first dispatch).
    # Individual item-level updates retain the full state machine for retries.
    if target_status == "IN_TRANSIT":
        eligible_sources = frozenset({"PENDING"})
    else:
        eligible_sources = frozenset(
            s for s, allowed in ALLOWED_TRANSITIONS.items() if target_status in allowed
        )

    all_items = db.execute(
        text("""
            SELECT
                ri.id            AS report_item_id,
                ri.invoice_number,
                COALESCE(du.status, 'PENDING') AS current_status
            FROM  report_items ri
            JOIN  reports        r  ON r.id = ri.report_id
            LEFT JOIN delivery_updates du ON du.report_item_id = ri.id
            WHERE r.manifest_number = :mn
        """),
        {"mn": manifest_number},
    ).fetchall()

    to_update = [row for row in all_items if row.current_status in eligible_sources]
    skipped   = len(all_items) - len(to_update)

    for row in to_update:
        # UPSERT — preserve existing notes and driver linkage via COALESCE,
        # consistent with the single-item PUT and existing bulk-confirm patterns.
        db.execute(
            text("""
                INSERT INTO delivery_updates
                    (report_item_id, invoice_number, manifest_number,
                     driver_user_id, driver_name, status, notes, updated_at)
                VALUES
                    (:report_item_id, :invoice_number, :manifest_number,
                     NULL, NULL, :target_status, NULL, CURRENT_TIMESTAMP)
                ON CONFLICT (report_item_id) DO UPDATE SET
                    status         = :target_status,
                    driver_user_id = COALESCE(delivery_updates.driver_user_id, EXCLUDED.driver_user_id),
                    driver_name    = COALESCE(delivery_updates.driver_name,    EXCLUDED.driver_name),
                    notes          = COALESCE(delivery_updates.notes,          EXCLUDED.notes),
                    updated_at     = CURRENT_TIMESTAMP
            """),
            {
                "report_item_id":  row.report_item_id,
                "invoice_number":  row.invoice_number,
                "manifest_number": manifest_number,
                "target_status":   target_status,
            },
        )

        du_id = db.execute(
            text("SELECT id FROM delivery_updates WHERE report_item_id = :id"),
            {"id": row.report_item_id},
        ).scalar()

        db.execute(
            text("""
                INSERT INTO delivery_events
                    (delivery_update_id, report_item_id, manifest_number,
                     invoice_number, status, previous_status, notes,
                     event_type, changed_by_user_id, changed_by_username, event_at)
                VALUES
                    (:du_id, :report_item_id, :manifest_number,
                     :invoice_number, :target_status, :previous_status, NULL,
                     :event_type, :user_id, :username, CURRENT_TIMESTAMP)
            """),
            {
                "du_id":           du_id,
                "report_item_id":  row.report_item_id,
                "manifest_number": manifest_number,
                "invoice_number":  row.invoice_number,
                "target_status":   target_status,
                "previous_status": row.current_status,
                "event_type":      DELIVERY_EVENT_STATUS_CHANGE,
                "user_id":         current_user["id"],
                "username":        current_user["username"],
            },
        )

    return {"updated": len(to_update), "skipped": skipped}


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

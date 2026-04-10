"""
app/services/manifest_service.py

Business logic for the manifest domain:
  - Orders / invoices (CRUD, search, staging)
  - Dispatch reports
  - Settings (drivers, routes, …)
  - Trucks
  - Customer-route mappings
  - Manifest audit events
"""

import os
import secrets
import threading
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from delivery_manifest_backend.app.db.database import get_db_session, execute_query, _is_sqlite
from delivery_manifest_backend.app.core.constants import (
    ORDER_TYPE_INVOICE, ORDER_TYPE_CREDIT_NOTE,
    INVOICE_STATUS_PENDING, INVOICE_STATUS_CANCELLED,
    INVOICE_STATUS_PROCESSED, INVOICE_STATUS_ORPHAN,
)
from delivery_manifest_backend.app.core.logger import get_logger

logger = get_logger(__name__)

# Serialises invoice‐import operations so the file watcher and the manual
# refresh endpoint cannot process the same folder concurrently.
_import_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# ORDERS
# ══════════════════════════════════════════════════════════════════════════════

def get_all_orders(allocated: bool = False) -> List[Dict]:
    """Return orders. allocated=False → pending only."""
    db = get_db_session()
    try:
        if allocated:
            result = execute_query(
                db,
                "SELECT * FROM orders WHERE type = ? ORDER BY date_processed DESC",
                (ORDER_TYPE_INVOICE,),
            )
        else:
            result = execute_query(
                db,
                """
                SELECT * FROM orders
                WHERE is_allocated = 0
                  AND type = ?
                  AND status != ?
                ORDER BY date_processed DESC
                """,
                (ORDER_TYPE_INVOICE, INVOICE_STATUS_CANCELLED),
            )
        return [dict(row._mapping) for row in result.fetchall()]
    finally:
        db.close()


def get_available_orders_excluding_staging(
    area: Optional[str] = None,
    limit: int = 2000,
    offset: int = 0,
) -> Tuple[List[Dict], int]:
    """
    Return unallocated orders not currently in any staging session.
    Returns (rows, total_count) to support pagination.

    NOT IN replaced with NOT EXISTS to prevent silent zero-row results:
    if manifest_staging.invoice_id ever contains NULL, NOT IN would return
    no rows at all — NOT EXISTS is NULL-safe and uses the FK index directly.
    """
    db = get_db_session()
    try:
        # Build the shared WHERE clause once; reused for COUNT and data query
        where = """
            FROM orders o
            WHERE NOT EXISTS (
                      SELECT 1 FROM manifest_staging ms
                      WHERE ms.invoice_id = o.id
                  )
              AND o.type = ?
              AND o.status != ?
              AND (
                    o.manifest_number IS NULL
                 OR NOT EXISTS (
                        SELECT 1 FROM reports r
                        WHERE r.manifest_number = o.manifest_number
                    )
              )
        """
        params: list = [ORDER_TYPE_INVOICE, INVOICE_STATUS_CANCELLED]

        # Optional area filter — pushed into SQL so pagination is accurate
        if area:
            where += " AND UPPER(o.area) = UPPER(?)"
            params.append(area)

        # Total count for pagination metadata (same WHERE, no LIMIT)
        total: int = execute_query(
            db, f"SELECT COUNT(*) {where}", params
        ).fetchone()[0]

        # Paginated data fetch
        data_query = f"SELECT o.* {where} ORDER BY COALESCE(NULLIF(o.invoice_date, 'N/A'), o.date_processed) DESC LIMIT ? OFFSET ?"
        rows = execute_query(db, data_query, params + [limit, offset]).fetchall()
        return [dict(row._mapping) for row in rows], total
    finally:
        db.close()


def get_order_by_filename(filename: str) -> Optional[Dict]:
    db = get_db_session()
    try:
        result = execute_query(db, "SELECT * FROM orders WHERE filename = ?", (filename,))
        row = result.fetchone()
        return dict(row._mapping) if row else None
    finally:
        db.close()


def get_order_by_invoice_number(invoice_number: str) -> Optional[Dict]:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "SELECT * FROM orders WHERE invoice_number = ? AND type = ?",
            (invoice_number, ORDER_TYPE_INVOICE),
        )
        row = result.fetchone()
        return dict(row._mapping) if row else None
    finally:
        db.close()


def _apply_credit_note(cn: Dict, invoice: Dict) -> bool:
    """
    Apply a single credit note to its referenced invoice.

    Determines full vs. partial credit by comparing values, then either
    cancels the invoice (full) or reduces its total_value (partial).
    Marks the credit note as PROCESSED in both cases.

    Returns True if reconciliation succeeded, False on value-parse error.
    """
    cn_number  = cn.get("invoice_number", "?")
    inv_number = invoice.get("invoice_number", "?")

    try:
        credit_val  = float(str(cn["total_value"]).replace(",", "").replace("$", "").strip())
        invoice_val = float(str(invoice["total_value"]).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        logger.error(
            f"[Reconcile] Cannot parse values — CN {cn_number}: {cn.get('total_value')!r}, "
            f"Invoice {inv_number}: {invoice.get('total_value')!r}"
        )
        return False

    if credit_val >= invoice_val - 0.01:
        # Full credit — cancel the invoice entirely
        cancel_order(inv_number)
        logger.info(
            f"[Reconcile] FULL CREDIT: cancelled invoice {inv_number} "
            f"(value {invoice_val:.2f}) via CN {cn_number}"
        )
    else:
        # Partial credit — reduce the invoice's outstanding value
        new_val      = invoice_val - credit_val
        original_val = invoice.get("original_value") or invoice["total_value"]
        update_order_value(inv_number, f"{new_val:.2f}", str(original_val))
        logger.info(
            f"[Reconcile] PARTIAL CREDIT: adjusted invoice {inv_number} "
            f"from {invoice_val:.2f} to {new_val:.2f} via CN {cn_number}"
        )

    # Mark the credit note itself as PROCESSED
    db = get_db_session()
    try:
        execute_query(
            db,
            "UPDATE orders SET status = ? WHERE id = ?",
            (INVOICE_STATUS_PROCESSED, cn["id"]),
        )
        db.commit()
    finally:
        db.close()

    return True


def _reconcile_orphans_for_invoice(invoice_number: str) -> int:
    """
    Post-import hook: called after a new INVOICE is saved.

    Queries for any ORPHAN credit notes that reference this invoice and
    reconciles each one immediately.  Safe to call even when no ORPHANs
    exist — it exits early without touching the database.

    Returns the number of credit notes reconciled.
    """
    # Fetch waiting ORPHAN credit notes for this invoice
    db = get_db_session()
    try:
        result = execute_query(
            db,
            """
            SELECT * FROM orders
            WHERE type            = ?
              AND status          = ?
              AND reference_number = ?
            """,
            (ORDER_TYPE_CREDIT_NOTE, INVOICE_STATUS_ORPHAN, invoice_number),
        )
        orphans = [dict(row._mapping) for row in result.fetchall()]
    finally:
        db.close()

    if not orphans:
        return 0

    # Re-fetch the invoice (it was just inserted; get_order_by_invoice_number
    # uses its own session so there's no stale-read risk)
    invoice = get_order_by_invoice_number(invoice_number)
    if not invoice:
        return 0

    reconciled = 0
    for cn in orphans:
        if _apply_credit_note(cn, invoice):
            reconciled += 1
            # Refresh invoice dict so a second partial CN sees the already-reduced value
            invoice = get_order_by_invoice_number(invoice_number) or invoice

    if reconciled:
        logger.info(
            f"[Reconcile] {reconciled} ORPHAN CN(s) reconciled against "
            f"invoice {invoice_number} on import."
        )
    return reconciled


def reconcile_all_orphans() -> int:
    """
    Startup sweep: iterate all ORPHAN credit notes and reconcile any whose
    referenced invoice now exists in the database.

    Safe to run on every application start — only touches rows with
    status = 'ORPHAN' that have a resolvable reference_number.
    Idempotent: already-PROCESSED credit notes are never re-touched.

    Returns the total number of credit notes reconciled.
    """
    db = get_db_session()
    try:
        result = execute_query(
            db,
            """
            SELECT * FROM orders
            WHERE type             = ?
              AND status           = ?
              AND reference_number IS NOT NULL
            ORDER BY id
            """,
            (ORDER_TYPE_CREDIT_NOTE, INVOICE_STATUS_ORPHAN),
        )
        orphans = [dict(row._mapping) for row in result.fetchall()]
    finally:
        db.close()

    if not orphans:
        logger.info("[Reconcile] Startup sweep: 0 ORPHAN credit notes found.")
        return 0

    logger.info(
        f"[Reconcile] Startup sweep: {len(orphans)} ORPHAN credit note(s) found — "
        "checking for resolvable matches…"
    )

    total = 0
    for cn in orphans:
        ref     = cn.get("reference_number")
        invoice = get_order_by_invoice_number(ref) if ref else None
        if not invoice:
            continue  # Invoice still not in DB — leave as ORPHAN
        if _apply_credit_note(cn, invoice):
            total += 1

    logger.info(f"[Reconcile] Startup sweep complete: {total} credit note(s) reconciled.")
    return total


def add_order(order_data: Dict) -> bool:
    """Insert a new order.  Returns False on duplicate filename or invoice_number."""
    inv_num = order_data.get("invoice_number", "N/A")
    db = get_db_session()
    try:
        # Pre-check: skip if this invoice_number already exists in the DB.
        # The partial unique index enforces this at the DB level too, but the
        # pre-check produces a clearer log message and avoids the INSERT round-trip.
        if inv_num and inv_num != "N/A":
            existing = db.execute(
                text("SELECT 1 FROM orders WHERE invoice_number = :inv"),
                {"inv": inv_num},
            ).fetchone()
            if existing:
                logger.info(
                    f"[SKIP] Invoice {inv_num} already in DB — "
                    f"skipping file {order_data.get('filename')}"
                )
                return False

        execute_query(
            db,
            """
            INSERT INTO orders
                (filename, date_processed, customer_name, total_value,
                 order_number, invoice_number, invoice_date, area,
                 type, reference_number, original_value, status, customer_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_data.get("filename"),
                order_data.get("date_processed", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                order_data.get("customer_name", "Unknown"),
                order_data.get("total_value", "0.00"),
                order_data.get("order_number", "N/A"),
                order_data.get("invoice_number", "N/A"),
                order_data.get("invoice_date", "N/A"),
                order_data.get("area", "UNKNOWN"),
                order_data.get("type", ORDER_TYPE_INVOICE),
                order_data.get("reference_number"),
                order_data.get("original_value"),
                order_data.get("status", INVOICE_STATUS_PENDING),
                order_data.get("customer_number", "N/A"),
            ),
        )
        db.commit()

        # Post-import hook: if this is a new invoice, immediately reconcile any
        # ORPHAN credit notes that arrived before it and have been waiting.
        if order_data.get("type", ORDER_TYPE_INVOICE) == ORDER_TYPE_INVOICE:
            if inv_num and inv_num != "N/A":
                _reconcile_orphans_for_invoice(inv_num)

        return True
    except IntegrityError:
        return False
    finally:
        db.close()


def allocate_orders(filenames: List[str], manifest_number: Optional[str] = None) -> int:
    """
    Mark orders as allocated.  Returns count updated.

    NOTE: This function is NOT wired to any API route and bypasses the
    staging workflow entirely.  It exists for internal/scripted use only.
    Do not expose it via a new endpoint — use save_report() instead, which
    runs allocation inside the finalisation transaction.
    """
    db = get_db_session()
    try:
        allocated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        placeholders   = ",".join(["?" for _ in filenames])
        result = execute_query(
            db,
            f"""
            UPDATE orders
            SET is_allocated = 1, allocated_date = ?, manifest_number = ?
            WHERE filename IN ({placeholders})
            """,
            [allocated_date, manifest_number] + filenames,
        )
        db.commit()
        return result.rowcount
    finally:
        db.close()


def deallocate_orders(filenames: List[str]) -> int:
    """
    Reset orders to pending and remove them from staging.  Returns count updated.

    Both the orders reset and the staging cleanup happen inside a single
    transaction so ghost staging rows cannot hide invoices from availability
    queries after a restore operation.
    """
    if not filenames:
        return 0
    db = get_db_session()
    try:
        placeholders = ",".join(["?" for _ in filenames])

        # Resolve filenames → order IDs for staging cleanup
        id_rows = execute_query(
            db,
            f"SELECT id FROM orders WHERE filename IN ({placeholders})",
            filenames,
        ).fetchall()
        invoice_ids = [r._mapping["id"] for r in id_rows]

        # Remove from staging (any session) in the same transaction
        if invoice_ids:
            id_ph = ",".join(["?" for _ in invoice_ids])
            execute_query(
                db,
                f"DELETE FROM manifest_staging WHERE invoice_id IN ({id_ph})",
                invoice_ids,
            )

        # Reset allocation flags
        result = execute_query(
            db,
            f"""
            UPDATE orders
            SET is_allocated = 0, allocated_date = NULL, manifest_number = NULL
            WHERE filename IN ({placeholders})
            """,
            filenames,
        )
        db.commit()
        return result.rowcount
    finally:
        db.close()


def cancel_order(invoice_number: str) -> bool:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "UPDATE orders SET status = ? WHERE invoice_number = ?",
            (INVOICE_STATUS_CANCELLED, invoice_number),
        )
        db.commit()
        return result.rowcount > 0
    finally:
        db.close()


def update_order_value(
    invoice_number: str, new_value: str, original_value: Optional[str] = None
) -> bool:
    db = get_db_session()
    try:
        if original_value:
            result = execute_query(
                db,
                "UPDATE orders SET total_value = ?, original_value = ? WHERE invoice_number = ?",
                (new_value, original_value, invoice_number),
            )
        else:
            result = execute_query(
                db,
                "UPDATE orders SET total_value = ? WHERE invoice_number = ?",
                (new_value, invoice_number),
            )
        db.commit()
        return result.rowcount > 0
    finally:
        db.close()


def search_orders(query: str) -> List[Dict]:
    """Full-text-style search across key invoice fields."""
    like = f"%{query}%"
    db = get_db_session()
    try:
        result = execute_query(
            db,
            """
            SELECT * FROM orders
            WHERE invoice_number  LIKE ?
               OR order_number    LIKE ?
               OR customer_name   LIKE ?
               OR filename        LIKE ?
               OR customer_number LIKE ?
            ORDER BY date_processed DESC
            LIMIT 50
            """,
            (like, like, like, like, like),
        )
        return [dict(row._mapping) for row in result.fetchall()]
    finally:
        db.close()


def get_areas() -> List[str]:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "SELECT DISTINCT area FROM orders WHERE area != 'UNKNOWN' ORDER BY area",
        )
        return [row._mapping["area"] for row in result.fetchall()]
    finally:
        db.close()


def get_all_customers() -> List[str]:
    db = get_db_session()
    try:
        # LIMIT 1000 — safety cap; realistic customer universe is well under this
        result = execute_query(
            db, "SELECT DISTINCT customer_name FROM orders ORDER BY customer_name LIMIT 1000"
        )
        return [row._mapping["customer_name"] for row in result.fetchall()]
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# MANIFEST STAGING
# ══════════════════════════════════════════════════════════════════════════════

def add_to_staging(session_id: str, filenames: List[str]) -> Dict:
    """
    Stage invoices for a manifest session.

    Returns a dict with four keys:
        added        – invoices newly staged this call
        already_yours – already in this session (skipped)
        taken        – staged by a different session (rejected)
        not_found    – filename not in orders table

    The UNIQUE(invoice_id) constraint on manifest_staging prevents the same
    invoice from being claimed by two sessions simultaneously.  The pre-check
    classifies each invoice before attempting the INSERT; any residual race is
    handled by INSERT OR IGNORE / ON CONFLICT DO NOTHING returning no row.
    """
    result: Dict = {"added": 0, "already_yours": 0, "taken": 0, "not_found": 0}
    if not filenames or not session_id:
        return result
    db = get_db_session()
    try:
        # 1. Resolve filenames → invoice IDs (single query)
        fn_ph = ",".join(["?" for _ in filenames])
        rows = execute_query(
            db,
            f"SELECT id FROM orders WHERE filename IN ({fn_ph})",
            filenames,
        ).fetchall()
        invoice_ids = [row._mapping["id"] for row in rows]
        result["not_found"] = len(filenames) - len(invoice_ids)

        if not invoice_ids:
            return result

        # 2. Single bulk check: which IDs are already staged, and by whom?
        id_ph = ",".join(["?" for _ in invoice_ids])
        staged_rows = execute_query(
            db,
            f"SELECT invoice_id, session_id FROM manifest_staging WHERE invoice_id IN ({id_ph})",
            invoice_ids,
        ).fetchall()
        staged_map = {r._mapping["invoice_id"]: r._mapping["session_id"] for r in staged_rows}

        # 3. Partition into categories; build insert list
        to_insert = []
        for invoice_id in invoice_ids:
            if invoice_id in staged_map:
                if staged_map[invoice_id] == session_id:
                    result["already_yours"] += 1
                else:
                    result["taken"] += 1
            else:
                to_insert.append(invoice_id)

        # 4. INSERT with conflict-safe syntax so races never raise exceptions.
        #    RETURNING id is appended by execute_query; a None fetchone() means
        #    the row was silently skipped (race — another session claimed it).
        if _is_sqlite:
            insert_sql = (
                "INSERT OR IGNORE INTO manifest_staging (session_id, invoice_id) VALUES (?, ?)"
            )
        else:
            insert_sql = (
                "INSERT INTO manifest_staging (session_id, invoice_id) VALUES (?, ?) "
                "ON CONFLICT (invoice_id) DO NOTHING"
            )

        for invoice_id in to_insert:
            r = execute_query(db, insert_sql, (session_id, invoice_id))
            if r.fetchone() is not None:
                result["added"] += 1
            else:
                result["taken"] += 1  # race: claimed between check and insert

        db.commit()
        return result
    finally:
        db.close()


def remove_from_staging(session_id: str, filenames: List[str]) -> int:
    """Remove invoices from staging.  Returns count removed."""
    if not filenames or not session_id:
        return 0
    db = get_db_session()
    try:
        placeholders = ",".join(["?" for _ in filenames])
        id_rows = execute_query(
            db,
            f"SELECT id FROM orders WHERE filename IN ({placeholders})",
            filenames,
        ).fetchall()
        invoice_ids = [r._mapping["id"] for r in id_rows]

        if not invoice_ids:
            return 0

        id_ph = ",".join(["?" for _ in invoice_ids])
        result = execute_query(
            db,
            f"DELETE FROM manifest_staging WHERE session_id = ? AND invoice_id IN ({id_ph})",
            [session_id] + invoice_ids,
        )
        removed = result.rowcount

        # Clear allocation flags for removed invoices (unless already in a finalised report)
        execute_query(
            db,
            f"""
            UPDATE orders
            SET is_allocated = 0, allocated_date = NULL, manifest_number = NULL
            WHERE id IN ({id_ph})
              AND (manifest_number IS NULL
                   OR manifest_number NOT IN (
                       SELECT DISTINCT manifest_number FROM reports
                   ))
            """,
            invoice_ids,
        )

        db.commit()
        return removed
    finally:
        db.close()


def get_current_manifest(
    session_id: str, manifest_number: Optional[str] = None
) -> List[Dict]:
    """Return invoices in the current manifest (finalised + staged)."""
    db = get_db_session()
    try:
        if manifest_number:
            result = execute_query(
                db,
                """
                SELECT o.* FROM orders o
                WHERE o.manifest_number = ? AND o.is_allocated = 1 AND o.type = 'INVOICE'
                UNION
                SELECT o.* FROM orders o
                INNER JOIN manifest_staging ms ON ms.invoice_id = o.id
                WHERE ms.session_id = ?
                  AND o.type = 'INVOICE'
                  AND o.is_allocated = 0
                  AND (o.manifest_number IS NULL OR o.manifest_number != ?)
                ORDER BY date_processed DESC
                """,
                (manifest_number, session_id, manifest_number),
            )
        else:
            result = execute_query(
                db,
                """
                SELECT o.* FROM orders o
                INNER JOIN manifest_staging ms ON ms.invoice_id = o.id
                WHERE ms.session_id = ? AND o.type = 'INVOICE' AND o.is_allocated = 0
                ORDER BY ms.added_at ASC
                """,
                (session_id,),
            )
        return [dict(row._mapping) for row in result.fetchall()]
    finally:
        db.close()


def clear_staging(session_id: str) -> int:
    db = get_db_session()
    try:
        result = execute_query(
            db, "DELETE FROM manifest_staging WHERE session_id = ?", (session_id,)
        )
        db.commit()
        return result.rowcount
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# MANIFEST NUMBER GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def generate_manifest_number(db, prefix: str = "A") -> str:
    """
    Atomically increment the manifest_counters row for *prefix* and return
    the next manifest number (e.g. "A35428").

    The caller MUST pass an active DB session.  The counter increment is
    flushed inside the caller's transaction — if that transaction rolls back,
    the incremented value is never committed and the slot is reclaimed.

    Raises RuntimeError if the counter row for *prefix* does not exist.

    Locking strategy
    ----------------
    PostgreSQL:
        SELECT ... FOR UPDATE acquires a row-level lock on the
        manifest_counters row before the read.  Any concurrent caller that
        reaches the same SELECT FOR UPDATE must wait until this transaction
        commits or rolls back.  The subsequent UPDATE then writes the new
        value under that lock.  This gives serialised, gap-free increments.

    SQLite:
        SQLite does not support SELECT FOR UPDATE.  Instead we use an
        atomic single-statement UPDATE that increments the counter
        in-place (last_number = last_number + 1) and then SELECT to
        read the just-written value within the same transaction.

        Why this is safe: SQLite allows only one writer at a time.
        When two concurrent sessions both attempt the UPDATE, SQLite
        serialises them at the write-lock boundary.  Each UPDATE reads
        the current committed (or within-transaction) value of last_number
        at execution time — not the value the session may have previously
        read with a SELECT — so both increments are applied sequentially
        and produce distinct results (e.g. 35428, then 35429).

        Accepted limitation: SQLite's write serialisation is at the
        database-file level, not row level.  Under very high concurrent
        write load a session may receive SQLITE_BUSY (mapped to
        OperationalError by SQLAlchemy).  This is acceptable for a
        single-server dispatch office application where concurrent
        manifest saves are rare.  Production uses PostgreSQL with
        proper row-level locking.
    """
    if _is_sqlite:
        # Atomic increment: SQLite serialises concurrent writes, so two
        # concurrent UPDATEs queue at the write-lock boundary.  Each reads
        # the already-committed value of last_number at execution time.
        result = db.execute(
            text("UPDATE manifest_counters SET last_number = last_number + 1 WHERE prefix = :p"),
            {"p": prefix},
        )
        if result.rowcount == 0:
            raise RuntimeError(
                f"Manifest counter row for prefix '{prefix}' not found. "
                f"Run database initialisation (init_db) to seed the manifest_counters table."
            )
        # Read the value we just wrote (within our own transaction — no other
        # session can see or modify it until we commit).
        row = db.execute(
            text("SELECT last_number FROM manifest_counters WHERE prefix = :p"),
            {"p": prefix},
        ).fetchone()
        return f"{prefix}{row[0]}"

    else:
        # PostgreSQL: lock the row before reading to prevent concurrent readers
        # from seeing the same value before we write.
        row = db.execute(
            text("SELECT last_number FROM manifest_counters WHERE prefix = :p FOR UPDATE"),
            {"p": prefix},
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"Manifest counter row for prefix '{prefix}' not found. "
                f"Run database initialisation (init_db) to seed the manifest_counters table."
            )
        next_number = row[0] + 1
        db.execute(
            text("UPDATE manifest_counters SET last_number = :n WHERE prefix = :p"),
            {"n": next_number, "p": prefix},
        )
        return f"{prefix}{next_number}"


# ══════════════════════════════════════════════════════════════════════════════
# REPORTS
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_user_id(db, username: Optional[str]) -> Optional[int]:
    """
    Look up a user ID by username from the users table.

    Returns the integer id if exactly one active user with that username
    exists, or None when the name is blank, not found, or the lookup fails.

    The lookup is unambiguous because usernames carry a UNIQUE constraint.
    No role filter is applied — driver and assistant accounts both use the
    DRIVER role, so filtering by role would be redundant and could silently
    break if roles change in the future.

    The caller owns the db session — this function must not commit or close it.
    """
    if not username or not username.strip():
        return None
    try:
        row = execute_query(
            db,
            "SELECT id FROM users WHERE username = ? AND is_active = TRUE LIMIT 1",
            (username.strip(),),
        ).fetchone()
        if row:
            return row[0]
        # Non-blank name that has no matching active user account — log clearly
        # so ops can spot unlinked assignments without inspecting the DB directly.
        logger.warning(
            f"[save_report] No active user account found for name={username!r}; "
            "driver_user_id / assistant_user_id will be NULL for this manifest."
        )
        return None
    except Exception:
        # Real DB failure — distinguish from "not found" at ERROR level.
        # Still returns None so the manifest save is not aborted.
        logger.error(
            f"[save_report] DB error during user ID lookup for name={username!r}; "
            "assignment FK will be NULL — manifest save continues.",
            exc_info=True,
        )
        return None


def save_report(report_data: Dict) -> dict:
    """
    Persist a dispatch report and finalise its staged invoices.

    The manifest number is generated atomically by the backend inside the
    same transaction that inserts the report.  Any client-supplied
    manifestNumber is ignored (transition safety).

    The authoritative invoice set comes from the server-side staging table
    joined to orders — the client-supplied invoices[] payload is used only
    as a supplement for per-line sku and weight, which are not stored in the
    orders table.  All other report_items fields (invoice_number, customer_name,
    value, etc.) come from the DB.  The report header totals (total_value,
    total_sku, total_weight) are computed from the saved line items; any
    frontend-supplied totalSku / totalWeight / totalValue fields are ignored.

    Assignment user ID resolution (Phase 2):
        driver_user_id    — prefer explicit value from payload; fall back to
                            username lookup against the users table.
        assistant_user_id — same preference/fallback logic.
        If the fallback lookup finds no matching active user the column is
        stored as NULL and the text column (driver / assistant) remains the
        only linkage — Phase 1 access-control already handles that case.

    Raises:
        ValueError  – session_id missing, or no staged invoices for this session.

    Returns {"id": report_id, "manifest_number": generated_number}.
    """
    session_id = report_data.get("session_id")
    if not session_id:
        raise ValueError("session_id is required to finalise a manifest")

    db = get_db_session()
    try:
        # ── Fetch authoritative staged invoice set ────────────────────────
        staged_rows = execute_query(
            db,
            """
            SELECT o.id, o.filename, o.invoice_number, o.order_number,
                   o.customer_name, o.invoice_date, o.area, o.customer_number,
                   o.total_value
            FROM orders o
            INNER JOIN manifest_staging ms ON ms.invoice_id = o.id
            WHERE ms.session_id = ?
            """,
            (session_id,),
        ).fetchall()

        if not staged_rows:
            raise ValueError(
                f"No staged invoices found for session '{session_id}' — "
                "cannot create an empty manifest"
            )

        staged_invoices  = [dict(row._mapping) for row in staged_rows]
        staged_filenames = [inv["filename"] for inv in staged_invoices]

        # ── Build lookup: frontend payload for sku/weight supplement ─────
        # These fields are not stored in the orders table and must come from
        # the frontend.  All other fields use the DB as authoritative source.
        frontend_map: Dict[str, Dict] = {}
        for inv in report_data.get("invoices", []):
            key = (inv.get("num") or inv.get("invoice_number") or "").strip()
            if key:
                frontend_map[key] = inv

        # ── Compute total_value from DB rows (authoritative) ─────────────
        db_total_value = sum(_parse_value(inv["total_value"]) for inv in staged_invoices)

        # ── Build line items; derive aggregate SKU/weight from saved lines ─
        # value comes from the DB orders.total_value (authoritative).
        # sku and weight are not in the orders table — pulled from the
        # frontend invoices[] payload.  Aggregate totals (total_sku,
        # total_weight) are then computed from the saved line values so the
        # report header is always consistent with its detail rows.
        # The frontend-supplied totalSku / totalWeight fields are ignored.
        line_items = []
        for inv in staged_invoices:
            inv_num = inv["invoice_number"] or "N/A"
            fe      = frontend_map.get(inv_num, {})
            line_items.append({
                "inv_num":         inv_num,
                "order_number":    inv["order_number"]    or "N/A",
                "customer_name":   inv["customer_name"]   or "N/A",
                "invoice_date":    inv["invoice_date"]    or "N/A",
                "area":            inv["area"]            or "UNKNOWN",
                "customer_number": inv["customer_number"] or "N/A",
                "sku":    int(fe.get("sku",    0) or 0),
                "value":  _parse_value(inv["total_value"]),  # from DB — authoritative
                "weight": float(fe.get("weight", 0) or 0),
            })

        db_total_sku    = sum(item["sku"]    for item in line_items)
        db_total_weight = sum(item["weight"] for item in line_items)

        # ── Generate manifest number inside the transaction ──────────────
        manifest_number = generate_manifest_number(db, prefix="A")

        # ── Resolve assignment user IDs (Phase 2) ────────────────────────
        # Prefer explicit IDs from payload; fall back to username lookup.
        # NULL is safe — Phase 1 access-control has text-name fallback.
        driver_name    = report_data.get("driver")
        assistant_name = report_data.get("assistant")

        driver_uid = report_data.get("driver_user_id")
        if driver_uid is None:
            driver_uid = _resolve_user_id(db, driver_name)

        assistant_uid = report_data.get("assistant_user_id")
        if assistant_uid is None:
            assistant_uid = _resolve_user_id(db, assistant_name)

        result = execute_query(
            db,
            """
            INSERT INTO reports
                (manifest_number, date, date_dispatched, driver, assistant, checker,
                 reg_number, pallets_brown, pallets_blue, crates, mileage,
                 total_value, total_sku, total_weight, created_at,
                 driver_user_id, assistant_user_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                manifest_number,
                report_data.get("date"),
                report_data.get("date"),
                driver_name,
                assistant_name,
                report_data.get("checker"),
                report_data.get("regNumber"),
                report_data.get("palletsBrown", 0),
                report_data.get("palletsBlue", 0),
                report_data.get("crates", 0),
                report_data.get("mileage", 0),
                db_total_value,   # authoritative: sum of DB invoice values
                db_total_sku,     # authoritative: sum of report_items.sku
                db_total_weight,  # authoritative: sum of report_items.weight
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                driver_uid,       # None when no matching active user found
                assistant_uid,    # None when no matching active user found
            ),
        )
        row = result.fetchone()
        report_id = row[0] if row else None

        if report_id is None:
            raise RuntimeError(
                f"Failed to insert report (manifest: {manifest_number})"
            )

        # ── Insert report_items ───────────────────────────────────────────
        for item in line_items:
            execute_query(
                db,
                """
                INSERT INTO report_items
                    (report_id, invoice_number, order_number, customer_name,
                     invoice_date, area, sku, value, weight, customer_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    item["inv_num"],
                    item["order_number"],
                    item["customer_name"],
                    item["invoice_date"],
                    item["area"],
                    item["sku"],
                    item["value"],
                    item["weight"],
                    item["customer_number"],
                ),
            )

        # ── Allocate all staged orders ─────────────────────────────────────
        ph = ",".join(["?" for _ in staged_filenames])
        allocated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        execute_query(
            db,
            f"""
            UPDATE orders
            SET is_allocated = 1, allocated_date = ?, manifest_number = ?
            WHERE filename IN ({ph})
            """,
            [allocated_date, manifest_number] + staged_filenames,
        )

        # ── Clean staging ─────────────────────────────────────────────────
        execute_query(
            db,
            "DELETE FROM manifest_staging WHERE session_id = ?",
            (session_id,),
        )

        db.commit()

        # Audit log (outside the main transaction)
        log_manifest_event(manifest_number, "CREATED", "System")
        return {"id": report_id, "manifest_number": manifest_number}

    except (ValueError, RuntimeError):
        db.rollback()
        raise
    finally:
        db.close()


def get_reports(
    date_from: Optional[str] = None, date_to: Optional[str] = None
) -> List[Dict]:
    """Return reports with their invoice items, optionally filtered by date."""
    db = get_db_session()
    try:
        query  = "SELECT * FROM reports"
        params: list = []

        conditions = []
        if date_from:
            conditions.append("date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("date <= ?")
            params.append(date_to + " 23:59:59")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY id DESC"

        reports = [dict(row._mapping) for row in execute_query(db, query, params).fetchall()]

        if reports:
            # Fetch ALL report items in a single query (eliminates the N+1 that
            # previously issued one SELECT per report).
            report_ids = [r["id"] for r in reports]
            report_map = {r["id"]: r for r in reports}
            for r in reports:
                r["invoices"] = []  # ensure key exists even if no items

            id_ph = ",".join(["?" for _ in report_ids])
            items = execute_query(
                db,
                f"SELECT * FROM report_items WHERE report_id IN ({id_ph}) ORDER BY report_id",
                report_ids,
            ).fetchall()
            for item in items:
                item_dict = dict(item._mapping)
                report_map[item_dict["report_id"]]["invoices"].append(item_dict)

        return reports
    finally:
        db.close()


def get_dispatched_invoices(
    date_from:    Optional[str] = None,
    date_to:      Optional[str] = None,
    filter_type:  str           = "dispatch",
    search_query: Optional[str] = None,
    route:        Optional[str] = None,
    limit:        int           = 50,
    offset:       int           = 0,
    sort_by:      str           = "date_dispatched",
    sort_order:   str           = "DESC",
) -> Tuple[List[Dict], int]:
    """
    Return invoice-level rows from dispatched manifests (paginated).
    Returns (results, total_count).
    """
    db = get_db_session()
    try:
        base = """
            SELECT
                r.manifest_number, r.date_dispatched, r.driver, r.assistant,
                r.checker, r.reg_number,
                ri.invoice_number, ri.order_number, ri.customer_name,
                ri.customer_number, ri.invoice_date, ri.area,
                ri.sku, ri.value, ri.weight,
                cr.route_name,
                cr.delivery_mode,
                du.status
            FROM reports r
            INNER JOIN report_items ri ON r.id = ri.report_id
            LEFT JOIN customer_routes cr ON ri.customer_name = cr.customer_name
            LEFT JOIN delivery_updates du ON du.report_item_id = ri.id
        """
        where: list[str] = []
        params: list     = []

        if date_from:
            where.append("r.date_dispatched >= ?")
            params.append(date_from)
        if date_to:
            where.append("r.date_dispatched <= ?")
            params.append(date_to)

        if search_query:
            sp = f"%{search_query}%"
            where.append(
                "(ri.invoice_number LIKE ? OR ri.order_number LIKE ? OR "
                "r.manifest_number LIKE ? OR ri.customer_name LIKE ? OR "
                "r.driver LIKE ? OR r.reg_number LIKE ? OR r.checker LIKE ?)"
            )
            params.extend([sp] * 7)

        if route:
            where.append("cr.route_name = ?")
            params.append(route)

        query = base + (" WHERE " + " AND ".join(where) if where else "")

        total = execute_query(
            db, f"SELECT COUNT(*) FROM ({query}) AS sub", params
        ).fetchone()[0]

        valid_sort = {
            "date_dispatched": "r.date_dispatched",
            "manifest_number": "r.manifest_number",
            "invoice_number":  "ri.invoice_number",
            "customer_name":   "ri.customer_name",
            "driver":          "r.driver",
        }
        sort_field = valid_sort.get(sort_by, "r.date_dispatched")
        order      = "DESC" if sort_order.upper() == "DESC" else "ASC"
        query     += f" ORDER BY {sort_field} {order} LIMIT ? OFFSET ?"
        params    += [limit, offset]

        rows = execute_query(db, query, params).fetchall()
        keys = [
            "manifest_number", "date_dispatched", "driver", "assistant",
            "checker", "reg_number", "invoice_number", "order_number",
            "customer_name", "customer_number", "invoice_date", "area",
            "sku", "value", "weight", "route_name", "delivery_mode", "delivery_status",
        ]
        results = [dict(zip(keys, row)) for row in rows]
        return results, total
    finally:
        db.close()


def get_outstanding_orders(
    limit: int = 500,
    offset: int = 0,
) -> Tuple[List[Dict], int]:
    """
    Return invoices that have never appeared in a dispatch report.
    Returns (rows, total_count) to support pagination.

    NOT IN replaced with NOT EXISTS — if report_items.invoice_number ever
    contains NULL, NOT IN returns zero rows silently.  NOT EXISTS is NULL-safe
    and uses the invoice_number index on report_items directly.
    """
    db = get_db_session()
    try:
        where = """
            FROM orders o
            WHERE NOT EXISTS (
                      SELECT 1 FROM report_items ri
                      WHERE ri.invoice_number = o.invoice_number
                  )
              AND o.status != ?
              AND o.type = ?
        """
        where_params = (INVOICE_STATUS_CANCELLED, ORDER_TYPE_INVOICE)

        total: int = execute_query(db, f"SELECT COUNT(*) {where}", where_params).fetchone()[0]

        result = execute_query(
            db,
            f"""
            SELECT o.invoice_number, o.order_number, o.customer_name, o.invoice_date,
                   o.customer_number, o.total_value, o.area
            {where}
            ORDER BY o.invoice_date DESC, o.invoice_number DESC
            LIMIT ? OFFSET ?
            """,
            (*where_params, limit, offset),
        )
        keys = [
            "invoice_number", "order_number", "customer_name", "invoice_date",
            "customer_number", "total_value", "area",
        ]
        return [dict(zip(keys, row)) for row in result.fetchall()], total
    finally:
        db.close()


def get_manifest_details(manifest_number: str) -> Optional[Dict]:
    """Return full manifest details including invoices and audit events."""
    db = get_db_session()
    try:
        row = execute_query(
            db, "SELECT * FROM reports WHERE manifest_number = ?", (manifest_number,)
        ).fetchone()
        if not row:
            return None

        data = dict(row._mapping)

        items = execute_query(
            db, "SELECT * FROM report_items WHERE report_id = ?", (data["id"],)
        ).fetchall()
        data["invoices"] = [dict(r._mapping) for r in items]

        events = execute_query(
            db,
            "SELECT * FROM manifest_events WHERE manifest_number = ? ORDER BY timestamp DESC",
            (manifest_number,),
        ).fetchall()
        data["events"] = [dict(r._mapping) for r in events]

        return data
    finally:
        db.close()


def log_manifest_event(
    manifest_number: str, event_type: str, performed_by: str = "System"
) -> bool:
    db = get_db_session()
    try:
        execute_query(
            db,
            """
            INSERT INTO manifest_events (manifest_number, event_type, performed_by, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (manifest_number, event_type, performed_by, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        db.commit()
        return True
    except Exception:
        logger.error("Error logging manifest event", exc_info=True)
        return False
    finally:
        db.close()



# ══════════════════════════════════════════════════════════════════════════════
# FILE OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════

def save_manifest_file(content: bytes, filename: str, folder: str) -> str:
    """Write manifest bytes to disk. Returns the destination path."""
    os.makedirs(folder, exist_ok=True)
    dest = os.path.join(folder, filename)
    with open(dest, "wb") as buf:
        buf.write(content)
    return dest


# ══════════════════════════════════════════════════════════════════════════════
# INVOICE PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

def refresh_invoices() -> None:
    """Trigger a re-scan of the invoice input folder via the invoice processor.

    Acquires ``_import_lock`` so the file watcher cannot process files
    concurrently (the ``importlib.reload`` is not thread-safe).
    """
    with _import_lock:
        import importlib
        import invoice_processor  # optional dependency; ImportError propagates to caller
        importlib.reload(invoice_processor)
        invoice_processor.main()


def _parse_value(value_str) -> float:
    """Parse a monetary string like '1,234.56' or '$1234.56' to float.  Returns 0.0 on error."""
    try:
        return float(str(value_str).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return 0.0


def create_manual_invoice(
    customer_name:   str,
    total_value:     str,
    invoice_number:  str,
    order_number:    str,
    customer_number: str,
    area:            str,
) -> Optional[str]:
    """
    Build and insert a manual invoice entry.

    Returns the generated filename on success, or None if the invoice
    number already exists (duplicate).
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"MANUAL_{ts}_{secrets.token_hex(4)}.pdf"
    ok = add_order(
        {
            "filename":        filename,
            "date_processed":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "customer_name":   customer_name,
            "total_value":     total_value,
            "invoice_number":  invoice_number,
            "order_number":    order_number,
            "customer_number": customer_number,
            "invoice_date":    datetime.now().strftime("%Y-%m-%d"),
            "area":            area,
        }
    )
    return filename if ok else None


def create_manual_invoice_and_stage(
    session_id:      str,
    customer_name:   str,
    total_value:     str,
    invoice_number:  str,
    order_number:    str,
    customer_number: str,
    area:            str,
) -> Optional[str]:
    """
    Atomically insert a manual invoice and stage it for *session_id*.

    Both the orders INSERT and the manifest_staging INSERT occur inside a
    single transaction — there is no window where the invoice exists in the
    DB but is not yet staged, eliminating the partial-failure hole in the
    previous two-step frontend workflow.

    Returns the generated filename on success, or None if the invoice
    number already exists (duplicate).
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"MANUAL_{ts}_{secrets.token_hex(4)}.pdf"
    db = get_db_session()
    try:
        # Pre-check for duplicate invoice_number to give a clean early exit
        if invoice_number and invoice_number != "N/A":
            existing = db.execute(
                text("SELECT 1 FROM orders WHERE invoice_number = :inv"),
                {"inv": invoice_number},
            ).fetchone()
            if existing:
                logger.info(
                    f"[Manual Invoice] Duplicate invoice_number {invoice_number!r} — skipping."
                )
                return None

        # INSERT order
        row = execute_query(
            db,
            """
            INSERT INTO orders
                (filename, date_processed, customer_name, total_value,
                 order_number, invoice_number, invoice_date, area,
                 type, reference_number, original_value, status, customer_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                filename,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                customer_name,
                total_value,
                order_number,
                invoice_number,
                datetime.now().strftime("%Y-%m-%d"),
                area,
                ORDER_TYPE_INVOICE,
                None,
                None,
                INVOICE_STATUS_PENDING,
                customer_number,
            ),
        ).fetchone()

        if row is None:
            raise RuntimeError(f"Failed to insert manual invoice {invoice_number!r}")
        invoice_id = row[0]

        # Stage in the same transaction — atomic with the INSERT above
        execute_query(
            db,
            "INSERT INTO manifest_staging (session_id, invoice_id) VALUES (?, ?)",
            (session_id, invoice_id),
        )

        db.commit()
        logger.info(
            f"[Manual Invoice] Created and staged {invoice_number!r} "
            f"(filename={filename}) for session '{session_id}'"
        )
        return filename

    except IntegrityError:
        db.rollback()
        logger.info(
            f"[Manual Invoice] IntegrityError for {invoice_number!r} — "
            "duplicate filename or invoice_number."
        )
        return None
    finally:
        db.close()

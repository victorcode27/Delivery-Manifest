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
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy.exc import IntegrityError

from app.db.database import get_db_session, execute_query
from app.core.logger import get_logger

logger = get_logger(__name__)


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
                "SELECT * FROM orders WHERE type = 'INVOICE' ORDER BY date_processed DESC",
            )
        else:
            result = execute_query(
                db,
                """
                SELECT * FROM orders
                WHERE is_allocated = 0
                  AND type = 'INVOICE'
                  AND status != 'CANCELLED'
                ORDER BY date_processed DESC
                """,
            )
        return [dict(row._mapping) for row in result.fetchall()]
    finally:
        db.close()


def get_available_orders_excluding_staging(
    area: Optional[str] = None,
    limit: int = 200,
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
              AND o.type = 'INVOICE'
              AND (
                    o.manifest_number IS NULL
                 OR NOT EXISTS (
                        SELECT 1 FROM reports r
                        WHERE r.manifest_number = o.manifest_number
                    )
              )
        """
        params: list = []

        # Optional area filter — pushed into SQL so pagination is accurate
        if area:
            where += " AND UPPER(o.area) = UPPER(?)"
            params.append(area)

        # Total count for pagination metadata (same WHERE, no LIMIT)
        total: int = execute_query(
            db, f"SELECT COUNT(*) {where}", params or None
        ).fetchone()[0]

        # Paginated data fetch
        data_query = f"SELECT o.* {where} ORDER BY o.date_processed DESC LIMIT ? OFFSET ?"
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
            "SELECT * FROM orders WHERE invoice_number = ? AND type = 'INVOICE'",
            (invoice_number,),
        )
        row = result.fetchone()
        return dict(row._mapping) if row else None
    finally:
        db.close()


def add_order(order_data: Dict) -> bool:
    """Insert a new order.  Returns False on duplicate filename."""
    db = get_db_session()
    try:
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
                order_data.get("type", "INVOICE"),
                order_data.get("reference_number"),
                order_data.get("original_value"),
                order_data.get("status", "PENDING"),
                order_data.get("customer_number", "N/A"),
            ),
        )
        db.commit()
        return True
    except IntegrityError:
        return False
    finally:
        db.close()


def allocate_orders(filenames: List[str], manifest_number: Optional[str] = None) -> int:
    """Mark orders as allocated.  Returns count updated."""
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
    """Reset orders to pending.  Returns count updated."""
    if not filenames:
        return 0
    db = get_db_session()
    try:
        placeholders = ",".join(["?" for _ in filenames])
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
            "UPDATE orders SET status = 'CANCELLED' WHERE invoice_number = ?",
            (invoice_number,),
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

def add_to_staging(session_id: str, filenames: List[str]) -> int:
    """
    Stage invoices for a manifest session.  Returns count added.

    Previously issued one SELECT per filename to check for duplicates (N+1).
    Now uses a single bulk existence check — 2 round trips regardless of
    how many invoices are being staged.
    """
    if not filenames or not session_id:
        return 0
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

        if not invoice_ids:
            return 0

        # 2. Single bulk check: which of these IDs are already staged?
        id_ph = ",".join(["?" for _ in invoice_ids])
        already_staged = {
            row._mapping["invoice_id"]
            for row in execute_query(
                db,
                f"SELECT invoice_id FROM manifest_staging "
                f"WHERE session_id = ? AND invoice_id IN ({id_ph})",
                [session_id] + invoice_ids,
            ).fetchall()
        }

        # 3. Insert only the IDs not yet in staging
        added = 0
        for invoice_id in invoice_ids:
            if invoice_id not in already_staged:
                execute_query(
                    db,
                    "INSERT INTO manifest_staging (session_id, invoice_id) VALUES (?, ?)",
                    (session_id, invoice_id),
                )
                added += 1

        db.commit()
        return added
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
# REPORTS
# ══════════════════════════════════════════════════════════════════════════════

def save_report(report_data: Dict) -> int:
    """Persist a dispatch report and finalise its staged invoices."""
    db = get_db_session()
    try:
        result = execute_query(
            db,
            """
            INSERT INTO reports
                (manifest_number, date, date_dispatched, driver, assistant, checker,
                 reg_number, pallets_brown, pallets_blue, crates, mileage,
                 total_value, total_sku, total_weight, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report_data.get("manifestNumber"),
                report_data.get("date"),
                report_data.get("date"),
                report_data.get("driver"),
                report_data.get("assistant"),
                report_data.get("checker"),
                report_data.get("regNumber"),
                report_data.get("palletsBrown", 0),
                report_data.get("palletsBlue", 0),
                report_data.get("crates", 0),
                report_data.get("mileage", 0),
                report_data.get("totalValue", 0),
                report_data.get("totalSku", 0),
                report_data.get("totalWeight", 0),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        report_id = result.inserted_primary_key[0] if getattr(result, "inserted_primary_key", None) else None

        # Persist invoice line items
        for inv in report_data.get("invoices", []):
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
                    inv.get("num") or inv.get("invoice_number", "N/A"),
                    inv.get("orderNum") or inv.get("order_number", "N/A"),
                    inv.get("customer") or inv.get("customer_name", "N/A"),
                    inv.get("invoiceDate") or inv.get("invoice_date", "N/A"),
                    inv.get("area", "UNKNOWN"),
                    inv.get("sku", 0),
                    inv.get("value", 0) or inv.get("total_value", 0),
                    inv.get("weight", 0),
                    inv.get("customerNumber") or inv.get("customer_number", "N/A"),
                ),
            )

        # Finalise staged invoices for this session
        session_id = report_data.get("session_id")
        if session_id:
            staged_rows = execute_query(
                db,
                """
                SELECT o.filename FROM orders o
                INNER JOIN manifest_staging ms ON ms.invoice_id = o.id
                WHERE ms.session_id = ?
                """,
                (session_id,),
            ).fetchall()
            staged_filenames = [r._mapping["filename"] for r in staged_rows]

            if staged_filenames:
                ph = ",".join(["?" for _ in staged_filenames])
                allocated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                execute_query(
                    db,
                    f"""
                    UPDATE orders
                    SET is_allocated = 1, allocated_date = ?, manifest_number = ?
                    WHERE filename IN ({ph})
                    """,
                    [allocated_date, report_data.get("manifestNumber")] + staged_filenames,
                )

            execute_query(
                db,
                "DELETE FROM manifest_staging WHERE session_id = ?",
                (session_id,),
            )

        db.commit()

        # Audit log (outside the main transaction)
        log_manifest_event(report_data.get("manifestNumber"), "CREATED", "System")
        return report_id
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
                ri.sku, ri.value, ri.weight
            FROM reports r
            INNER JOIN report_items ri ON r.id = ri.report_id
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
            "sku", "value", "weight",
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
              AND o.status != 'CANCELLED'
              AND o.type = 'INVOICE'
        """

        total: int = execute_query(db, f"SELECT COUNT(*) {where}").fetchone()[0]

        result = execute_query(
            db,
            f"""
            SELECT o.invoice_number, o.order_number, o.customer_name, o.invoice_date,
                   o.customer_number, o.total_value, o.area
            {where}
            ORDER BY o.invoice_date DESC, o.invoice_number DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
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
# SETTINGS
# ══════════════════════════════════════════════════════════════════════════════

def get_settings(category: str) -> List[str]:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "SELECT value FROM settings WHERE category = ? ORDER BY value",
            (category,),
        )
        return [row._mapping["value"] for row in result.fetchall()]
    finally:
        db.close()


def add_setting(category: str, value: str) -> bool:
    db = get_db_session()
    try:
        execute_query(
            db, "INSERT INTO settings (category, value) VALUES (?, ?)", (category, value)
        )
        db.commit()
        return True
    except IntegrityError:
        return False
    finally:
        db.close()


def update_setting(category: str, old_value: str, new_value: str) -> bool:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "UPDATE settings SET value = ? WHERE category = ? AND value = ?",
            (new_value, category, old_value),
        )
        db.commit()
        return result.rowcount > 0
    except IntegrityError:
        return False
    finally:
        db.close()


def delete_setting(category: str, value: str) -> bool:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "DELETE FROM settings WHERE category = ? AND value = ?",
            (category, value),
        )
        db.commit()
        return result.rowcount > 0
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# TRUCKS
# ══════════════════════════════════════════════════════════════════════════════

def get_trucks() -> List[Dict]:
    db = get_db_session()
    try:
        result = execute_query(db, "SELECT * FROM trucks ORDER BY reg")
        return [dict(row._mapping) for row in result.fetchall()]
    finally:
        db.close()


def add_truck(
    reg: str,
    driver: Optional[str] = None,
    assistant: Optional[str] = None,
    checker: Optional[str] = None,
) -> bool:
    db = get_db_session()
    try:
        execute_query(
            db,
            "INSERT INTO trucks (reg, driver, assistant, checker) VALUES (?, ?, ?, ?)",
            (reg, driver, assistant, checker),
        )
        db.commit()
        return True
    except IntegrityError:
        return False
    finally:
        db.close()


def update_truck(
    reg: str,
    driver: Optional[str] = None,
    assistant: Optional[str] = None,
    checker: Optional[str] = None,
) -> bool:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "UPDATE trucks SET driver = ?, assistant = ?, checker = ? WHERE reg = ?",
            (driver, assistant, checker, reg),
        )
        db.commit()
        return result.rowcount > 0
    finally:
        db.close()


def delete_truck(reg: str) -> bool:
    db = get_db_session()
    try:
        result = execute_query(db, "DELETE FROM trucks WHERE reg = ?", (reg,))
        db.commit()
        return result.rowcount > 0
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# CUSTOMER ROUTES
# ══════════════════════════════════════════════════════════════════════════════

def get_customer_routes() -> Dict[str, str]:
    db = get_db_session()
    try:
        result = execute_query(
            db, "SELECT customer_name, route_name FROM customer_routes"
        )
        return {r._mapping["customer_name"]: r._mapping["route_name"] for r in result.fetchall()}
    finally:
        db.close()


def add_customer_route(customer_name: str, route_name: str) -> bool:
    db = get_db_session()
    try:
        execute_query(
            db,
            "REPLACE INTO customer_routes (customer_name, route_name) VALUES (?, ?)",
            (customer_name, route_name),
        )
        db.commit()
        return True
    except IntegrityError:
        return False
    finally:
        db.close()


def delete_customer_route(customer_name: str) -> bool:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "DELETE FROM customer_routes WHERE customer_name = ?",
            (customer_name,),
        )
        db.commit()
        return result.rowcount > 0
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
    """Trigger a re-scan of the invoice input folder via the invoice processor."""
    import importlib
    import invoice_processor  # optional dependency; ImportError propagates to caller
    importlib.reload(invoice_processor)
    invoice_processor.main()


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

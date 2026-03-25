"""
app/routes/analytics.py

Phase 1 analytics endpoints.

Routes
------
  GET /api/analytics/overview    — fleet-wide KPI summary
  GET /api/analytics/manifests   — paginated manifest-level delivery breakdown
  GET /api/analytics/drivers     — paginated driver performance summary
  GET /api/analytics/exceptions  — paginated FAILED/PARTIAL/RETURNED/MISSING_POD invoices
  GET /api/analytics/aging       — unresolved invoice and manifest age buckets

Auth
----
  All endpoints: require_office_read (ADMIN, DISPATCH, REPORTS_ONLY; DRIVER blocked)

Data sources
------------
  Current-state source: delivery_updates (LEFT JOIN on report_items.id)
  Effective status:     COALESCE(du.status, 'PENDING')
  delivery_events is NOT used for Phase 1 calculations (audit log only)

Driver grouping rule (canonical — see delivery_service.py)
  1. PRIMARY:  reports.driver_user_id  (FK → users.id)
  2. FALLBACK: reports.driver text     (when driver_user_id IS NULL)
  3. UNKNOWN:  'Unassigned'            (both NULL / blank / 'N/A')
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from delivery_manifest_backend.app.core.deps import require_office_read
from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.db.database import get_db

router = APIRouter(prefix="/analytics", tags=["analytics"])
logger = get_logger(__name__)


# ── Filter helpers ─────────────────────────────────────────────────────────────

def _date_filters(conditions: list, params: dict, date_from, date_to) -> None:
    if date_from:
        conditions.append("r.date_dispatched >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("r.date_dispatched <= :date_to")
        params["date_to"] = date_to


def _assignment_filters(
    conditions: list, params: dict,
    driver_user_id=None, driver=None, assistant=None,
) -> None:
    if driver_user_id is not None:
        conditions.append("r.driver_user_id = :driver_user_id")
        params["driver_user_id"] = driver_user_id
    elif driver:
        conditions.append("LOWER(r.driver) LIKE LOWER(:driver_like)")
        params["driver_like"] = f"%{driver}%"
    if assistant:
        conditions.append("LOWER(r.assistant) LIKE LOWER(:assistant_like)")
        params["assistant_like"] = f"%{assistant}%"


def _route_filter(conditions: list, params: dict, route) -> None:
    if route:
        conditions.append("cr.route_name = :route")
        params["route"] = route


def _where(conditions: list) -> str:
    return ("WHERE " + " AND ".join(conditions)) if conditions else ""


# ── Reusable SQL fragments ─────────────────────────────────────────────────────

# Status counts + PoD counts using effective status.
# Produces aliases: pending, in_transit, delivered, failed, partial, returned,
#                   pod_count, missing_pod_count
_STATUS_COUNTS = """
    SUM(CASE WHEN COALESCE(du.status,'PENDING') = 'PENDING'    THEN 1 ELSE 0 END) AS pending,
    SUM(CASE WHEN COALESCE(du.status,'PENDING') = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit,
    SUM(CASE WHEN COALESCE(du.status,'PENDING') = 'DELIVERED'  THEN 1 ELSE 0 END) AS delivered,
    SUM(CASE WHEN COALESCE(du.status,'PENDING') = 'FAILED'     THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN COALESCE(du.status,'PENDING') = 'PARTIAL'    THEN 1 ELSE 0 END) AS partial,
    SUM(CASE WHEN COALESCE(du.status,'PENDING') = 'RETURNED'   THEN 1 ELSE 0 END) AS returned,
    SUM(CASE WHEN COALESCE(du.status,'PENDING') = 'DELIVERED'
             AND du.pod_image_path IS NOT NULL
             AND du.pod_image_path != ''         THEN 1 ELSE 0 END) AS pod_count,
    SUM(CASE WHEN COALESCE(du.status,'PENDING') = 'DELIVERED'
             AND (du.pod_image_path IS NULL OR du.pod_image_path = '')
                                                 THEN 1 ELSE 0 END) AS missing_pod_count
"""

# Manifest-level derived status — mirrors delivery_service.derive_manifest_status().
# References alias names produced by _STATUS_COUNTS plus total_invoices.
# Must be used in an outer SELECT (CTE or subquery) where those aliases are columns.
_MANIFEST_STATUS = """
    CASE
      WHEN total_invoices = 0 OR total_invoices = pending
        THEN 'PENDING'
      WHEN total_invoices = delivered + returned
        THEN 'COMPLETED'
      WHEN pending + in_transit = 0 AND failed + partial > 0
        THEN 'COMPLETED_WITH_ISSUES'
      ELSE 'IN_PROGRESS'
    END
"""

# Standard FROM / JOIN block shared by overview, manifests, exceptions, aging.
# Drivers uses a variant that also joins users for display name resolution.
_BASE_FROM = """
    FROM  reports            r
    JOIN  report_items      ri ON ri.report_id     = r.id
    LEFT JOIN delivery_updates  du ON du.report_item_id = ri.id
    LEFT JOIN customer_routes   cr ON cr.customer_name  = ri.customer_name
"""

# Age bucket expression — days since dispatch for unresolved invoices.
# PostgreSQL: CURRENT_DATE - CAST(text AS DATE) returns INTEGER days.
_AGE_BUCKET = """
    CASE
      WHEN CURRENT_DATE - CAST(r.date_dispatched AS DATE) <= 1  THEN '0-1'
      WHEN CURRENT_DATE - CAST(r.date_dispatched AS DATE) <= 3  THEN '2-3'
      WHEN CURRENT_DATE - CAST(r.date_dispatched AS DATE) <= 7  THEN '4-7'
      WHEN CURRENT_DATE - CAST(r.date_dispatched AS DATE) <= 14 THEN '8-14'
      ELSE '15+'
    END
"""

# Canonical driver display name — FK-preferred, text fallback, 'Unassigned' sentinel.
# Used in both SELECT and GROUP BY in the drivers endpoint.
_DRIVER_DISPLAY = """
    CASE
      WHEN r.driver_user_id IS NOT NULL
        THEN COALESCE(u.username, r.driver, 'Unassigned')
      WHEN r.driver IS NULL OR TRIM(r.driver) = '' OR r.driver = 'N/A'
        THEN 'Unassigned'
      ELSE r.driver
    END
"""


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/analytics/overview
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/overview")
def analytics_overview(
    date_from:      Optional[str] = None,
    date_to:        Optional[str] = None,
    driver_user_id: Optional[int] = None,
    driver:         Optional[str] = None,
    assistant:      Optional[str] = None,
    route:          Optional[str] = None,
    db:             Session       = Depends(get_db),
    current_user:   dict          = Depends(require_office_read),
):
    conditions: list = []
    params:     dict = {}

    _date_filters(conditions, params, date_from, date_to)
    _assignment_filters(conditions, params, driver_user_id, driver, assistant)
    _route_filter(conditions, params, route)

    where_sql = _where(conditions)

    try:
        row = db.execute(text(f"""
            SELECT
                COUNT(DISTINCT r.id)                                              AS total_manifests,
                COUNT(ri.id)                                                      AS total_invoices,
                {_STATUS_COUNTS},
                SUM(CASE WHEN COALESCE(du.status,'PENDING') IN ('PENDING','IN_TRANSIT')
                         THEN 1 ELSE 0 END)                                       AS unresolved_count,
                SUM(CASE WHEN COALESCE(du.status,'PENDING') IN ('FAILED','PARTIAL','RETURNED')
                         THEN 1 ELSE 0 END)                                       AS exception_count,
                SUM(CASE WHEN COALESCE(du.status,'PENDING') IN ('DELIVERED','FAILED','PARTIAL','RETURNED')
                         THEN 1 ELSE 0 END)                                       AS resolved_count
            {_BASE_FROM}
            {where_sql}
        """), params).fetchone()
    except Exception:
        logger.error("analytics/overview query failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    total_invoices = row.total_invoices or 0
    delivered      = row.delivered      or 0
    pod_count      = row.pod_count      or 0

    completion_rate     = round(delivered / total_invoices * 100, 1) if total_invoices else 0.0
    pod_compliance_rate = round(pod_count / delivered * 100, 1)      if delivered      else 0.0

    return {
        "total_manifests":     row.total_manifests   or 0,
        "total_invoices":      total_invoices,
        "pending":             row.pending            or 0,
        "in_transit":          row.in_transit         or 0,
        "delivered":           delivered,
        "failed":              row.failed             or 0,
        "partial":             row.partial            or 0,
        "returned":            row.returned           or 0,
        "unresolved_count":    row.unresolved_count   or 0,
        "exception_count":     row.exception_count    or 0,
        "resolved_count":      row.resolved_count     or 0,
        "pod_count":           pod_count,
        "missing_pod_count":   row.missing_pod_count  or 0,
        "completion_rate":     completion_rate,
        "pod_compliance_rate": pod_compliance_rate,
    }


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/analytics/manifests
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/manifests")
def analytics_manifests(
    date_from:      Optional[str] = None,
    date_to:        Optional[str] = None,
    driver_user_id: Optional[int] = None,
    driver:         Optional[str] = None,
    assistant:      Optional[str] = None,
    route:          Optional[str] = None,
    status:         Optional[str] = None,
    search:         Optional[str] = None,
    page:           int           = 1,
    page_size:      int           = 50,
    db:             Session       = Depends(get_db),
    current_user:   dict          = Depends(require_office_read),
):
    page      = max(1, page)
    page_size = max(1, min(200, page_size))
    offset    = (page - 1) * page_size

    conditions: list = []
    params:     dict = {}

    _date_filters(conditions, params, date_from, date_to)
    _assignment_filters(conditions, params, driver_user_id, driver, assistant)
    _route_filter(conditions, params, route)

    if search:
        conditions.append("""(
            LOWER(r.manifest_number) LIKE LOWER(:search)
            OR LOWER(r.driver)       LIKE LOWER(:search)
            OR LOWER(r.assistant)    LIKE LOWER(:search)
            OR LOWER(cr.route_name)  LIKE LOWER(:search)
        )""")
        params["search"] = f"%{search}%"

    where_sql = _where(conditions)

    # Aggregate CTE — one row per manifest with full status breakdown.
    # route: MIN(cr.route_name) used as a representative value since manifests
    # can span multiple customer routes; note this in any UI tooltip.
    agg_cte = f"""
        SELECT
            r.id                  AS report_id,
            r.manifest_number,
            r.date_dispatched     AS dispatch_date,
            r.driver,
            r.driver_user_id,
            r.assistant,
            MIN(cr.route_name)    AS route,
            COUNT(ri.id)          AS total_invoices,
            {_STATUS_COUNTS},
            SUM(CASE WHEN COALESCE(du.status,'PENDING') IN ('PENDING','IN_TRANSIT')
                     THEN 1 ELSE 0 END) AS unresolved_count,
            SUM(CASE WHEN COALESCE(du.status,'PENDING') IN ('FAILED','PARTIAL','RETURNED')
                     THEN 1 ELSE 0 END) AS exception_count,
            SUM(CASE WHEN COALESCE(du.status,'PENDING') IN ('DELIVERED','FAILED','PARTIAL','RETURNED')
                     THEN 1 ELSE 0 END) AS resolved_count,
            MAX(du.updated_at)    AS last_update_at
        {_BASE_FROM}
        {where_sql}
        GROUP BY r.id, r.manifest_number, r.date_dispatched,
                 r.driver, r.driver_user_id, r.assistant
    """

    try:
        if status:
            status_filter = status.upper()
            params["status_filter"] = status_filter
            total = db.execute(text(f"""
                WITH agg AS ({agg_cte}),
                with_status AS (
                    SELECT *, {_MANIFEST_STATUS} AS manifest_status FROM agg
                )
                SELECT COUNT(*) FROM with_status
                WHERE manifest_status = :status_filter
            """), params).scalar() or 0
        else:
            total = db.execute(text(f"""
                SELECT COUNT(DISTINCT r.id)
                {_BASE_FROM}
                {where_sql}
            """), params).scalar() or 0

        status_where = "WHERE manifest_status = :status_filter" if status else ""

        rows = db.execute(text(f"""
            WITH agg AS ({agg_cte}),
            with_status AS (
                SELECT *, {_MANIFEST_STATUS} AS manifest_status FROM agg
            )
            SELECT * FROM with_status
            {status_where}
            ORDER BY dispatch_date DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """), {**params, "limit": page_size, "offset": offset}).fetchall()

    except Exception:
        logger.error("analytics/manifests query failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    items = []
    for r in rows:
        total_inv      = r.total_invoices or 0
        delivered      = r.delivered      or 0
        pod_count      = r.pod_count      or 0
        completion_pct = round(delivered / total_inv * 100, 1) if total_inv else 0.0

        items.append({
            "manifest_number":   r.manifest_number,
            "report_id":         r.report_id,
            "dispatch_date":     r.dispatch_date,
            "driver":            r.driver,
            "driver_user_id":    r.driver_user_id,
            "assistant":         r.assistant,
            "route":             r.route,
            "total_invoices":    total_inv,
            "pending":           r.pending           or 0,
            "in_transit":        r.in_transit        or 0,
            "delivered":         delivered,
            "failed":            r.failed            or 0,
            "partial":           r.partial           or 0,
            "returned":          r.returned          or 0,
            "unresolved_count":  r.unresolved_count  or 0,
            "exception_count":   r.exception_count   or 0,
            "resolved_count":    r.resolved_count    or 0,
            "pod_count":         pod_count,
            "missing_pod_count": r.missing_pod_count or 0,
            "completion_pct":    completion_pct,
            "manifest_status":   r.manifest_status,
            "last_update_at":    str(r.last_update_at) if r.last_update_at else None,
        })

    return {"total": total, "page": page, "page_size": page_size, "items": items}


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/analytics/drivers
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/drivers")
def analytics_drivers(
    date_from:  Optional[str] = None,
    date_to:    Optional[str] = None,
    route:      Optional[str] = None,
    assistant:  Optional[str] = None,
    search:     Optional[str] = None,
    page:       int           = 1,
    page_size:  int           = 50,
    db:         Session       = Depends(get_db),
    current_user: dict        = Depends(require_office_read),
):
    page      = max(1, page)
    page_size = max(1, min(200, page_size))
    offset    = (page - 1) * page_size

    conditions: list = []
    params:     dict = {}

    _date_filters(conditions, params, date_from, date_to)
    if assistant:
        conditions.append("LOWER(r.assistant) LIKE LOWER(:assistant_like)")
        params["assistant_like"] = f"%{assistant}%"
    _route_filter(conditions, params, route)

    where_sql = _where(conditions)

    # CTE aggregates one row per canonical driver identity.
    # GROUP BY (driver_user_id, _DRIVER_DISPLAY expression) ensures:
    #   - All manifests with the same FK user_id collapse into one group
    #     even when the driver text column differs across rows.
    #   - Text-only records group by their normalised display name.
    agg_cte = f"""
        SELECT
            r.driver_user_id,
            {_DRIVER_DISPLAY}                                     AS driver,
            COUNT(DISTINCT r.id)                                  AS manifests_assigned,
            COUNT(ri.id)                                          AS total_invoices,
            {_STATUS_COUNTS},
            SUM(CASE WHEN COALESCE(du.status,'PENDING') IN ('PENDING','IN_TRANSIT')
                     THEN 1 ELSE 0 END)                           AS unresolved_count,
            SUM(CASE WHEN COALESCE(du.status,'PENDING') IN ('FAILED','PARTIAL','RETURNED')
                     THEN 1 ELSE 0 END)                           AS exception_count,
            SUM(CASE WHEN COALESCE(du.status,'PENDING') IN ('DELIVERED','FAILED','PARTIAL','RETURNED')
                     THEN 1 ELSE 0 END)                           AS resolved_count,
            MAX(du.updated_at)                                    AS last_activity_at
        FROM  reports            r
        JOIN  report_items      ri ON ri.report_id     = r.id
        LEFT JOIN delivery_updates  du ON du.report_item_id = ri.id
        LEFT JOIN customer_routes   cr ON cr.customer_name  = ri.customer_name
        LEFT JOIN users              u ON u.id              = r.driver_user_id
        {where_sql}
        GROUP BY r.driver_user_id, {_DRIVER_DISPLAY}
    """

    search_where = ""
    if search:
        search_where = "WHERE LOWER(driver) LIKE LOWER(:search)"
        params["search"] = f"%{search}%"

    try:
        total = db.execute(text(f"""
            WITH agg AS ({agg_cte})
            SELECT COUNT(*) FROM agg {search_where}
        """), params).scalar() or 0

        rows = db.execute(text(f"""
            WITH agg AS ({agg_cte})
            SELECT * FROM agg
            {search_where}
            ORDER BY total_invoices DESC, driver ASC NULLS LAST
            LIMIT :limit OFFSET :offset
        """), {**params, "limit": page_size, "offset": offset}).fetchall()

    except Exception:
        logger.error("analytics/drivers query failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    items = []
    for r in rows:
        total_inv  = r.total_invoices or 0
        delivered  = r.delivered      or 0
        pod_count  = r.pod_count      or 0
        success_rate        = round(delivered / total_inv * 100, 1) if total_inv else 0.0
        pod_compliance_rate = round(pod_count / delivered * 100, 1) if delivered else 0.0

        items.append({
            "driver":              r.driver,
            "driver_user_id":      r.driver_user_id,
            "manifests_assigned":  r.manifests_assigned  or 0,
            "total_invoices":      total_inv,
            "pending":             r.pending             or 0,
            "in_transit":          r.in_transit          or 0,
            "delivered":           delivered,
            "failed":              r.failed              or 0,
            "partial":             r.partial             or 0,
            "returned":            r.returned            or 0,
            "unresolved_count":    r.unresolved_count    or 0,
            "exception_count":     r.exception_count     or 0,
            "resolved_count":      r.resolved_count      or 0,
            "pod_count":           pod_count,
            "missing_pod_count":   r.missing_pod_count   or 0,
            "success_rate":        success_rate,
            "pod_compliance_rate": pod_compliance_rate,
            "last_activity_at":    str(r.last_activity_at) if r.last_activity_at else None,
        })

    return {"total": total, "page": page, "page_size": page_size, "items": items}


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/analytics/exceptions
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/exceptions")
def analytics_exceptions(
    date_from:      Optional[str] = None,
    date_to:        Optional[str] = None,
    driver_user_id: Optional[int] = None,
    driver:         Optional[str] = None,
    assistant:      Optional[str] = None,
    route:          Optional[str] = None,
    status:         Optional[str] = None,
    search:         Optional[str] = None,
    page:           int           = 1,
    page_size:      int           = 50,
    db:             Session       = Depends(get_db),
    current_user:   dict          = Depends(require_office_read),
):
    page      = max(1, page)
    page_size = max(1, min(200, page_size))
    offset    = (page - 1) * page_size

    conditions: list = []
    params:     dict = {}

    _date_filters(conditions, params, date_from, date_to)
    _assignment_filters(conditions, params, driver_user_id, driver, assistant)
    _route_filter(conditions, params, route)

    # Status filter logic:
    #   MISSING_POD — DELIVERED invoices with no pod_image_path recorded
    #   FAILED / PARTIAL / RETURNED — that specific exception status
    #   default (no param) — all three exception statuses combined
    status_upper = status.upper() if status else None

    if status_upper == "MISSING_POD":
        conditions.append("COALESCE(du.status,'PENDING') = 'DELIVERED'")
        conditions.append("(du.pod_image_path IS NULL OR du.pod_image_path = '')")
    elif status_upper in ("FAILED", "PARTIAL", "RETURNED"):
        conditions.append("COALESCE(du.status,'PENDING') = :exc_status")
        params["exc_status"] = status_upper
    else:
        conditions.append("COALESCE(du.status,'PENDING') IN ('FAILED','PARTIAL','RETURNED')")

    if search:
        conditions.append("""(
            LOWER(ri.invoice_number)   LIKE LOWER(:search)
            OR LOWER(r.manifest_number) LIKE LOWER(:search)
            OR LOWER(ri.customer_name)  LIKE LOWER(:search)
        )""")
        params["search"] = f"%{search}%"

    where_sql = _where(conditions)

    # customer_routes.customer_name is UNIQUE so the LEFT JOIN produces at most
    # one cr row per ri row — no GROUP BY needed for de-duplication.
    try:
        total = db.execute(text(f"""
            SELECT COUNT(ri.id)
            {_BASE_FROM}
            {where_sql}
        """), params).scalar() or 0

        rows = db.execute(text(f"""
            SELECT
                ri.id                                                              AS report_item_id,
                ri.invoice_number,
                r.manifest_number,
                r.date_dispatched                                                  AS dispatch_date,
                ri.customer_name,
                ri.area,
                r.driver,
                r.driver_user_id,
                r.assistant,
                cr.route_name                                                      AS route,
                COALESCE(du.status,'PENDING')                                      AS status,
                du.notes,
                (du.pod_image_path IS NOT NULL AND du.pod_image_path != '')        AS pod_present,
                (du.signature_path  IS NOT NULL AND du.signature_path  != '')      AS signature_present,
                du.updated_at,
                CURRENT_DATE - CAST(r.date_dispatched AS DATE)                     AS age_days
            {_BASE_FROM}
            {where_sql}
            ORDER BY r.date_dispatched DESC NULLS LAST, ri.id ASC
            LIMIT :limit OFFSET :offset
        """), {**params, "limit": page_size, "offset": offset}).fetchall()

    except Exception:
        logger.error("analytics/exceptions query failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    items = []
    for r in rows:
        items.append({
            "report_item_id":    r.report_item_id,
            "invoice_number":    r.invoice_number,
            "manifest_number":   r.manifest_number,
            "dispatch_date":     r.dispatch_date,
            "customer_name":     r.customer_name,
            "area":              r.area,
            "driver":            r.driver,
            "driver_user_id":    r.driver_user_id,
            "assistant":         r.assistant,
            "route":             r.route,
            "status":            r.status,
            "notes":             r.notes,
            "pod_present":       bool(r.pod_present),
            "signature_present": bool(r.signature_present),
            "updated_at":        str(r.updated_at) if r.updated_at else None,
            "age_days":          r.age_days,
        })

    return {"total": total, "page": page, "page_size": page_size, "items": items}


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/analytics/aging
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/aging")
def analytics_aging(
    date_from:      Optional[str] = None,
    date_to:        Optional[str] = None,
    driver_user_id: Optional[int] = None,
    driver:         Optional[str] = None,
    assistant:      Optional[str] = None,
    route:          Optional[str] = None,
    db:             Session       = Depends(get_db),
    current_user:   dict          = Depends(require_office_read),
):
    conditions: list = []
    params:     dict = {}

    _date_filters(conditions, params, date_from, date_to)
    _assignment_filters(conditions, params, driver_user_id, driver, assistant)
    _route_filter(conditions, params, route)

    # Aging only applies to unresolved invoices
    conditions.append("COALESCE(du.status,'PENDING') IN ('PENDING','IN_TRANSIT')")

    where_sql = _where(conditions)

    try:
        invoice_rows = db.execute(text(f"""
            SELECT
                {_AGE_BUCKET}                                                      AS bucket,
                COUNT(ri.id)                                                       AS invoice_count,
                COALESCE(SUM(ri.value), 0)                                         AS total_value,
                SUM(CASE WHEN COALESCE(du.status,'PENDING')='PENDING'    THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN COALESCE(du.status,'PENDING')='IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit
            {_BASE_FROM}
            {where_sql}
            GROUP BY bucket
        """), params).fetchall()

        manifest_rows = db.execute(text(f"""
            SELECT
                {_AGE_BUCKET}        AS bucket,
                COUNT(DISTINCT r.id) AS manifest_count
            {_BASE_FROM}
            {where_sql}
            GROUP BY bucket
        """), params).fetchall()

    except Exception:
        logger.error("analytics/aging query failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    _BUCKETS = ["0-1", "2-3", "4-7", "8-14", "15+"]

    invoice_map  = {r.bucket: r for r in invoice_rows}
    manifest_map = {r.bucket: r for r in manifest_rows}

    invoice_aging  = []
    manifest_aging = []

    for b in _BUCKETS:
        ir = invoice_map.get(b)
        invoice_aging.append({
            "bucket":        b,
            "invoice_count": ir.invoice_count or 0 if ir else 0,
            "total_value":   round(float(ir.total_value or 0), 2) if ir else 0.0,
            "pending":       ir.pending    or 0 if ir else 0,
            "in_transit":    ir.in_transit or 0 if ir else 0,
        })

        mr = manifest_map.get(b)
        manifest_aging.append({
            "bucket":         b,
            "manifest_count": mr.manifest_count or 0 if mr else 0,
        })

    return {
        "invoice_aging":  invoice_aging,
        "manifest_aging": manifest_aging,
    }


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/analytics/routes
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/routes")
def analytics_routes(
    date_from: Optional[str] = None,
    date_to:   Optional[str] = None,
    route:     Optional[str] = None,
    search:    Optional[str] = None,
    db:        Session       = Depends(get_db),
    current_user: dict       = Depends(require_office_read),
):
    # ── Shared filters: date range + route ────────────────────────────────
    # Both queries use _BASE_FROM (includes customer_routes LEFT JOIN) so the
    # route filter (cr.route_name = :route) and date filter apply uniformly.
    base_conditions: list = []
    base_params:     dict = {}

    _date_filters(base_conditions, base_params, date_from, date_to)
    _route_filter(base_conditions, base_params, route)

    # ── Route-specific conditions (search on route_name) ──────────────────
    route_conditions = list(base_conditions)
    route_params     = dict(base_params)
    if search:
        route_conditions.append(
            "LOWER(COALESCE(cr.route_name, 'Unmapped')) LIKE LOWER(:search_like)"
        )
        route_params["search_like"] = f"%{search}%"
    route_where = _where(route_conditions)

    # ── Area-specific conditions (search on area) ─────────────────────────
    area_conditions = list(base_conditions)
    area_params     = dict(base_params)
    if search:
        area_conditions.append(
            "LOWER(COALESCE(ri.area, 'UNKNOWN')) LIKE LOWER(:search_like)"
        )
        area_params["search_like"] = f"%{search}%"
    area_where = _where(area_conditions)

    try:
        route_rows = db.execute(text(f"""
            SELECT
                COALESCE(cr.route_name, 'Unmapped')                     AS route_name,
                MIN(cr.delivery_mode)                                    AS delivery_mode,
                COUNT(ri.id)                                             AS total_invoices,
                {_STATUS_COUNTS},
                SUM(CASE WHEN COALESCE(du.status,'PENDING')
                              IN ('FAILED','PARTIAL','RETURNED')
                         THEN 1 ELSE 0 END)                              AS exception_count
            {_BASE_FROM}
            {route_where}
            GROUP BY COALESCE(cr.route_name, 'Unmapped')
            ORDER BY total_invoices DESC
        """), route_params).fetchall()

        area_rows = db.execute(text(f"""
            SELECT
                CASE WHEN ri.area IS NULL OR TRIM(ri.area) = ''
                     THEN 'UNKNOWN' ELSE ri.area END                     AS area,
                COUNT(ri.id)                                             AS total_invoices,
                {_STATUS_COUNTS},
                SUM(CASE WHEN COALESCE(du.status,'PENDING')
                              IN ('FAILED','PARTIAL','RETURNED')
                         THEN 1 ELSE 0 END)                              AS exception_count
            {_BASE_FROM}
            {area_where}
            GROUP BY CASE WHEN ri.area IS NULL OR TRIM(ri.area) = ''
                          THEN 'UNKNOWN' ELSE ri.area END
            ORDER BY total_invoices DESC
        """), area_params).fetchall()

    except Exception:
        logger.error("analytics/routes query failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    route_results = []
    for r in route_rows:
        total_inv           = r.total_invoices or 0
        delivered           = r.delivered      or 0
        pod_count           = r.pod_count      or 0
        completion_rate     = round(delivered / total_inv * 100, 1) if total_inv else 0.0
        pod_compliance_rate = round(pod_count / delivered * 100, 1) if delivered else 0.0
        route_results.append({
            "route_name":          r.route_name,
            "delivery_mode":       r.delivery_mode,
            "total_invoices":      total_inv,
            "pending":             r.pending           or 0,
            "in_transit":          r.in_transit        or 0,
            "delivered":           delivered,
            "failed":              r.failed            or 0,
            "partial":             r.partial           or 0,
            "returned":            r.returned          or 0,
            "exception_count":     r.exception_count   or 0,
            "pod_count":           pod_count,
            "missing_pod_count":   r.missing_pod_count or 0,
            "completion_rate":     completion_rate,
            "pod_compliance_rate": pod_compliance_rate,
        })

    area_results = []
    for r in area_rows:
        total_inv       = r.total_invoices or 0
        delivered       = r.delivered      or 0
        completion_rate = round(delivered / total_inv * 100, 1) if total_inv else 0.0
        area_results.append({
            "area":            r.area,
            "total_invoices":  total_inv,
            "pending":         r.pending         or 0,
            "in_transit":      r.in_transit       or 0,
            "delivered":       delivered,
            "failed":          r.failed           or 0,
            "partial":         r.partial          or 0,
            "returned":        r.returned         or 0,
            "exception_count": r.exception_count  or 0,
            "completion_rate": completion_rate,
        })

    return {"route_results": route_results, "area_results": area_results}


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/analytics/trends
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/trends")
def analytics_trends(
    date_from:    Optional[str] = None,
    date_to:      Optional[str] = None,
    route:        Optional[str] = None,
    granularity:  Optional[str] = "day",
    db:           Session       = Depends(get_db),
    current_user: dict          = Depends(require_office_read),
):
    gran = (granularity or "day").lower()
    if gran not in ("day", "week"):
        gran = "day"

    conditions: list = []
    params:     dict = {}

    _date_filters(conditions, params, date_from, date_to)
    _route_filter(conditions, params, route)

    where_sql = _where(conditions)

    period_expr = (
        "DATE_TRUNC('week', CAST(r.date_dispatched AS DATE))"
        if gran == "week"
        else "CAST(r.date_dispatched AS DATE)"
    )

    try:
        rows = db.execute(text(f"""
            SELECT
                {period_expr}        AS period,
                COUNT(DISTINCT r.id) AS manifests_dispatched,
                COUNT(ri.id)         AS invoices_dispatched,
                {_STATUS_COUNTS}
            {_BASE_FROM}
            {where_sql}
            GROUP BY {period_expr}
            ORDER BY period DESC
            LIMIT 365
        """), params).fetchall()
    except Exception:
        logger.error("analytics/trends query failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    series = []
    for r in rows:
        pending    = r.pending    or 0
        in_transit = r.in_transit or 0
        failed     = r.failed     or 0
        partial    = r.partial    or 0
        returned   = r.returned   or 0

        series.append({
            "period":               str(r.period)[:10] if r.period else None,
            "manifests_dispatched": r.manifests_dispatched or 0,
            "invoices_dispatched":  r.invoices_dispatched  or 0,
            "delivered":            r.delivered            or 0,
            "unresolved":           pending + in_transit,
            "exceptions":           failed + partial + returned,
            "pod_uploads":          r.pod_count            or 0,
        })

    series.reverse()  # restore chronological (ASC) order after DESC fetch
    return {"granularity": gran, "series": series}

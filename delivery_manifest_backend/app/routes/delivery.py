"""
app/routes/delivery.py

Delivery execution endpoints.

Routes
------
  GET  /api/delivery/manifests                        — list assigned manifests with summary counts
  GET  /api/delivery/manifests/{manifest_number}      — full invoice list for one manifest
  PUT  /api/delivery/updates/{report_item_id}         — UPSERT delivery status + notes; appends audit row
  POST /api/delivery/updates/{report_item_id}/pod     — upload PoD file; stores path in delivery_updates
  GET  /api/delivery/files/{file_path}                — serve an uploaded PoD file (auth required)

Auth matrix
-----------
  DRIVER       own manifests only (matched by driver_user_id FK or driver text fallback)
  DISPATCH     all manifests, read + write
  ADMIN        all manifests, read + write
  REPORTS_ONLY blocked (403) — not in require_delivery_access
"""

import os
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from delivery_manifest_backend.app.core.config import settings
from delivery_manifest_backend.app.core.deps import get_current_user, require_delivery_access, require_delivery_read
from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.db.database import get_db
from delivery_manifest_backend.app.schemas.delivery import (
    DeliveryManifestDetailResponse,
    DeliveryManifestItem,
    DeliveryManifestListResponse,
    DeliveryManifestSummary,
    DeliveryStatusSummary,
    DeliveryUpdateRequest,
    DeliveryUpdateResponse,
    PodUploadResponse,
)

# ── PoD file validation helpers ────────────────────────────────────────────────

# (magic_bytes, file_extension, mime_type)
_MAGIC_TYPES = [
    (b'\xff\xd8\xff',              'jpg', 'image/jpeg'),
    (b'\x89\x50\x4e\x47\x0d\x0a', 'png', 'image/png'),
    (b'\x25\x50\x44\x46',         'pdf', 'application/pdf'),
]

_SAFE_RE = re.compile(r'[^\w-]')   # keep alphanumeric, underscore, hyphen


def _detect_file_type(header: bytes):
    """Return (ext, mime) for a recognised file header, or (None, None)."""
    for magic, ext, mime in _MAGIC_TYPES:
        if header.startswith(magic):
            return ext, mime
    return None, None

router = APIRouter(prefix="/delivery", tags=["delivery"])
logger = get_logger(__name__)


# ── Private helpers ────────────────────────────────────────────────────────────

def _derive_manifest_status(statuses: list) -> str:
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


def _is_manifest_assigned_to_driver(
    db: Session, manifest_number: str, user: dict
) -> bool:
    """
    Return True if the manifest is assigned to the given DRIVER user.

    Checks both the FK (driver_user_id) and the legacy text column (driver)
    so the feature works before drivers are formally linked to accounts.
    """
    result = db.execute(
        text("""
            SELECT 1
            FROM   reports
            WHERE  manifest_number = :mn
              AND  (driver_user_id = :uid OR driver = :uname)
            LIMIT  1
        """),
        {"mn": manifest_number, "uid": user["id"], "uname": user["username"]},
    )
    return result.fetchone() is not None


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/delivery/manifests
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/manifests", response_model=DeliveryManifestListResponse)
def list_manifests(
    date_from:    Optional[str] = None,
    date_to:      Optional[str] = None,
    db:           Session       = Depends(get_db),
    current_user: dict          = Depends(require_delivery_read),
):
    """
    Return manifests with per-manifest delivery summary counts.

    DRIVER     — only manifests assigned to them (driver_user_id FK or driver text fallback).
    ADMIN/DISPATCH — all manifests, optionally filtered by date range.

    Query params
    ------------
    date_from : YYYY-MM-DD  lower bound on date_dispatched (inclusive)
    date_to   : YYYY-MM-DD  upper bound on date_dispatched (inclusive)
    """
    role = current_user.get("role")

    conditions: list = []
    params:     dict = {}

    if role == "DRIVER":
        conditions.append("(r.driver_user_id = :uid OR r.driver = :uname)")
        params["uid"]   = current_user["id"]
        params["uname"] = current_user["username"]

    if date_from:
        conditions.append("r.date_dispatched >= :date_from")
        params["date_from"] = date_from

    if date_to:
        conditions.append("r.date_dispatched <= :date_to")
        params["date_to"] = date_to

    where_sql = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    sql = text(f"""
        SELECT
            r.manifest_number,
            r.date_dispatched,
            r.driver,
            r.reg_number,
            COUNT(ri.id)                                                            AS total_items,
            SUM(CASE WHEN COALESCE(du.status, 'PENDING') = 'DELIVERED'  THEN 1 ELSE 0 END) AS delivered,
            SUM(CASE WHEN COALESCE(du.status, 'PENDING') = 'PENDING'    THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN COALESCE(du.status, 'PENDING') = 'FAILED'     THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN COALESCE(du.status, 'PENDING') = 'PARTIAL'    THEN 1 ELSE 0 END) AS partial,
            SUM(CASE WHEN COALESCE(du.status, 'PENDING') = 'RETURNED'   THEN 1 ELSE 0 END) AS returned,
            SUM(CASE WHEN COALESCE(du.status, 'PENDING') = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit
        FROM  reports r
        LEFT JOIN report_items    ri ON ri.report_id     = r.id
        LEFT JOIN delivery_updates du ON du.report_item_id = ri.id
        {where_sql}
        GROUP BY r.manifest_number, r.date_dispatched, r.driver, r.reg_number
        ORDER BY r.date_dispatched DESC NULLS LAST
    """)

    try:
        rows = db.execute(sql, params).fetchall()
    except Exception:
        logger.error("Error listing delivery manifests", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    manifests = []
    for row in rows:
        # Reconstruct the full status list so _derive_manifest_status gets
        # accurate input even though we only stored aggregated counts.
        statuses = (
            ["DELIVERED"]  * (row.delivered  or 0) +
            ["PENDING"]    * (row.pending    or 0) +
            ["FAILED"]     * (row.failed     or 0) +
            ["PARTIAL"]    * (row.partial    or 0) +
            ["RETURNED"]   * (row.returned   or 0) +
            ["IN_TRANSIT"] * (row.in_transit or 0)
        )
        summary = DeliveryStatusSummary(
            status     = _derive_manifest_status(statuses),
            delivered  = row.delivered  or 0,
            pending    = row.pending    or 0,
            failed     = row.failed     or 0,
            partial    = row.partial    or 0,
            returned   = row.returned   or 0,
            in_transit = row.in_transit or 0,
        )
        manifests.append(DeliveryManifestSummary(
            manifest_number  = row.manifest_number,
            date_dispatched  = row.date_dispatched,
            driver           = row.driver,
            reg_number       = row.reg_number,
            total_items      = row.total_items or 0,
            delivery_summary = summary,
        ))

    logger.info(
        f"Listed {len(manifests)} delivery manifests for "
        f"'{current_user['username']}' (role={role})"
    )
    return DeliveryManifestListResponse(manifests=manifests, total=len(manifests))


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/delivery/manifests/{manifest_number}
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/manifests/{manifest_number}", response_model=DeliveryManifestDetailResponse)
def get_manifest_detail(
    manifest_number: str,
    db:              Session = Depends(get_db),
    current_user:    dict    = Depends(require_delivery_read),
):
    """
    Return the full invoice list for a manifest with per-invoice delivery status.

    DRIVER     — 403 if manifest is not assigned to them.
    ADMIN/DISPATCH — unrestricted.

    If no delivery_updates row exists for an invoice, delivery_status is 'PENDING'.
    """
    role = current_user.get("role")

    if role == "DRIVER" and not _is_manifest_assigned_to_driver(
        db, manifest_number, current_user
    ):
        raise HTTPException(
            status_code=403,
            detail="Access denied: manifest not assigned to you",
        )

    # Manifest header
    header = db.execute(
        text("""
            SELECT manifest_number, date_dispatched, driver
            FROM   reports
            WHERE  manifest_number = :mn
            LIMIT  1
        """),
        {"mn": manifest_number},
    ).fetchone()

    if not header:
        raise HTTPException(status_code=404, detail="Manifest not found")

    # Invoice rows with LEFT JOIN on delivery_updates
    try:
        rows = db.execute(
            text("""
                SELECT
                    ri.id                              AS report_item_id,
                    ri.invoice_number,
                    ri.customer_name,
                    ri.customer_number,
                    ri.area,
                    ri.value,
                    COALESCE(du.status, 'PENDING')     AS delivery_status,
                    du.notes,
                    du.pod_image_path,
                    du.signature_path,
                    du.updated_at
                FROM  report_items    ri
                JOIN  reports          r  ON r.id           = ri.report_id
                LEFT JOIN delivery_updates du ON du.report_item_id = ri.id
                WHERE r.manifest_number = :mn
                ORDER BY ri.id
            """),
            {"mn": manifest_number},
        ).fetchall()
    except Exception:
        logger.error(
            f"Error fetching manifest detail for '{manifest_number}'", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")

    items    = []
    statuses = []
    for row in rows:
        statuses.append(row.delivery_status)
        items.append(DeliveryManifestItem(
            report_item_id  = row.report_item_id,
            invoice_number  = row.invoice_number,
            customer_name   = row.customer_name,
            customer_number = row.customer_number,
            area            = row.area,
            value           = row.value,
            delivery_status = row.delivery_status,
            notes           = row.notes,
            has_pod         = bool(row.pod_image_path),
            has_signature   = bool(row.signature_path),
            pod_image_path  = row.pod_image_path,
            updated_at      = str(row.updated_at) if row.updated_at else None,
        ))

    return DeliveryManifestDetailResponse(
        manifest_number = header.manifest_number,
        driver          = header.driver,
        date_dispatched = header.date_dispatched,
        manifest_status = _derive_manifest_status(statuses),
        items           = items,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PUT /api/delivery/updates/{report_item_id}
# ══════════════════════════════════════════════════════════════════════════════

@router.put("/updates/{report_item_id}", response_model=DeliveryUpdateResponse)
def update_delivery_status(
    report_item_id: int,
    body:           DeliveryUpdateRequest,
    db:             Session = Depends(get_db),
    current_user:   dict    = Depends(require_delivery_access),
):
    """
    UPSERT delivery status and notes for a single invoice.

    Creates the delivery_updates row if it does not exist.
    Appends an entry to delivery_events (audit trail) on every call.

    DRIVER     — 403 if the item belongs to a manifest not assigned to them.
    ADMIN/DISPATCH — can update any item.

    DRIVER updates store their user ID and username in the driver fields.
    ADMIN/DISPATCH updates preserve whatever driver linkage already exists
    (NULL on first insert, existing value on subsequent updates via COALESCE).
    """
    role = current_user.get("role")

    # Resolve report_item → manifest header in one query
    item_row = db.execute(
        text("""
            SELECT ri.id, ri.invoice_number, r.manifest_number, r.driver, r.driver_user_id
            FROM   report_items ri
            JOIN   reports       r ON r.id = ri.report_id
            WHERE  ri.id = :id
        """),
        {"id": report_item_id},
    ).fetchone()

    if not item_row:
        raise HTTPException(status_code=404, detail="Report item not found")

    # DRIVER access check
    if role == "DRIVER":
        assigned = (
            item_row.driver_user_id == current_user["id"]
            or item_row.driver == current_user["username"]
        )
        if not assigned:
            raise HTTPException(
                status_code=403,
                detail="Access denied: item not in your manifest",
            )

    # Determine driver fields to store.
    # DRIVER: writes their own ID + username.
    # ADMIN/DISPATCH: passes NULL so COALESCE preserves existing values on conflict,
    # and NULL is stored on first insert (driver linkage not assumed by office staff).
    if role == "DRIVER":
        store_driver_uid  = current_user["id"]
        store_driver_name = current_user["username"]
    else:
        store_driver_uid  = None
        store_driver_name = None

    try:
        # UPSERT — ON CONFLICT on the UNIQUE constraint of report_item_id.
        # COALESCE logic on conflict:
        #   driver_user_id: keep existing if new value is NULL (ADMIN/DISPATCH edits)
        #   driver_name:    keep existing if new value is NULL (ADMIN/DISPATCH edits)
        db.execute(
            text("""
                INSERT INTO delivery_updates
                    (report_item_id, invoice_number, manifest_number,
                     driver_user_id, driver_name, status, notes, updated_at)
                VALUES
                    (:report_item_id, :invoice_number, :manifest_number,
                     :driver_uid, :driver_name, :status, :notes, CURRENT_TIMESTAMP)
                ON CONFLICT (report_item_id) DO UPDATE SET
                    status         = EXCLUDED.status,
                    notes          = EXCLUDED.notes,
                    driver_user_id = COALESCE(EXCLUDED.driver_user_id, delivery_updates.driver_user_id),
                    driver_name    = COALESCE(EXCLUDED.driver_name,    delivery_updates.driver_name),
                    updated_at     = CURRENT_TIMESTAMP
            """),
            {
                "report_item_id":  report_item_id,
                "invoice_number":  item_row.invoice_number,
                "manifest_number": item_row.manifest_number,
                "driver_uid":      store_driver_uid,
                "driver_name":     store_driver_name,
                "status":          body.status,
                "notes":           body.notes,
            },
        )

        # Fetch the row after upsert to get the current id for the audit event
        du_row = db.execute(
            text("SELECT * FROM delivery_updates WHERE report_item_id = :id"),
            {"id": report_item_id},
        ).fetchone()

        # Audit trail — append-only
        db.execute(
            text("""
                INSERT INTO delivery_events
                    (delivery_update_id, report_item_id, manifest_number, invoice_number,
                     status, notes, changed_by_user_id, changed_by_username, event_at)
                VALUES
                    (:du_id, :report_item_id, :manifest_number, :invoice_number,
                     :status, :notes, :user_id, :username, CURRENT_TIMESTAMP)
            """),
            {
                "du_id":           du_row.id,
                "report_item_id":  report_item_id,
                "manifest_number": item_row.manifest_number,
                "invoice_number":  item_row.invoice_number,
                "status":          body.status,
                "notes":           body.notes,
                "user_id":         current_user["id"],
                "username":        current_user["username"],
            },
        )

        db.commit()

    except HTTPException:
        raise
    except Exception:
        db.rollback()
        logger.error(
            f"Error updating delivery status for report_item_id={report_item_id}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Internal server error")

    logger.info(
        f"Delivery update: item={report_item_id} manifest={item_row.manifest_number} "
        f"status={body.status} by '{current_user['username']}' (role={role})"
    )

    return DeliveryUpdateResponse(
        id              = du_row.id,
        report_item_id  = du_row.report_item_id,
        invoice_number  = du_row.invoice_number,
        manifest_number = du_row.manifest_number,
        driver_user_id  = du_row.driver_user_id,
        driver_name     = du_row.driver_name,
        status          = du_row.status,
        notes           = du_row.notes,
        updated_at      = str(du_row.updated_at) if du_row.updated_at else None,
    )


# ══════════════════════════════════════════════════════════════════════════════
# POST /api/delivery/updates/{report_item_id}/pod
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/updates/{report_item_id}/pod", response_model=PodUploadResponse)
async def upload_pod(
    report_item_id: int,
    pod_file:       UploadFile = File(...),
    db:             Session    = Depends(get_db),
    current_user:   dict       = Depends(require_delivery_access),
):
    """
    Upload a Proof of Delivery file for a single invoice.

    Accepted types : JPEG, PNG, PDF (validated by magic bytes, not extension).
    Size limit      : settings.POD_MAX_BYTES (default 8 MB).
    Storage         : {UPLOADS_ROOT}/pods/{manifest_number}/{timestamp}_{invoice}_{name}.{ext}
    DB              : delivery_updates.pod_image_path ← relative path from UPLOADS_ROOT.
    Atomic write    : DB commit first → disk write second.
                      DB is reverted if disk write fails.

    DRIVER     — 403 if item not in their assigned manifest.
    ADMIN/DISPATCH — unrestricted.
    """
    role = current_user.get("role")

    # Resolve report_item → manifest header
    item_row = db.execute(
        text("""
            SELECT ri.id, ri.invoice_number, r.manifest_number, r.driver, r.driver_user_id
            FROM   report_items ri
            JOIN   reports       r ON r.id = ri.report_id
            WHERE  ri.id = :id
        """),
        {"id": report_item_id},
    ).fetchone()

    if not item_row:
        raise HTTPException(status_code=404, detail="Report item not found")

    # DRIVER access check — same logic as PUT status endpoint
    if role == "DRIVER":
        assigned = (
            item_row.driver_user_id == current_user["id"]
            or item_row.driver == current_user["username"]
        )
        if not assigned:
            raise HTTPException(
                status_code=403,
                detail="Access denied: item not in your manifest",
            )

    # Read header bytes first (needed for type detection without loading whole file)
    header = await pod_file.read(8)
    ext, mime = _detect_file_type(header)
    if not ext:
        raise HTTPException(
            status_code=422,
            detail="Unsupported file type. Allowed: JPEG, PNG, PDF",
        )

    # Read remainder to check total size
    remainder = await pod_file.read()
    total_size = len(header) + len(remainder)
    if total_size > settings.POD_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum {settings.POD_MAX_BYTES // (1024 * 1024)} MB allowed",
        )

    # Build safe file name: YYYYMMDDHHMMSS_invoiceNumber_originalStem.ext
    ts         = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    safe_inv   = _SAFE_RE.sub('_', item_row.invoice_number)[:30]
    orig_stem  = os.path.splitext(pod_file.filename or 'file')[0]
    safe_stem  = _SAFE_RE.sub('_', orig_stem)[:20]
    filename   = f"{ts}_{safe_inv}_{safe_stem}.{ext}"

    rel_dir  = os.path.join('pods', item_row.manifest_number)
    rel_path = os.path.join(rel_dir, filename).replace('\\', '/')   # forward slashes in DB
    abs_dir  = os.path.join(os.path.realpath(settings.UPLOADS_ROOT), 'pods', item_row.manifest_number)
    abs_path = os.path.join(abs_dir, filename)

    os.makedirs(abs_dir, exist_ok=True)

    # ── Atomic write: DB commit first, disk second ──────────────────────────
    try:
        # UPSERT — only touch pod_image_path and updated_at on conflict
        db.execute(
            text("""
                INSERT INTO delivery_updates
                    (report_item_id, invoice_number, manifest_number, status, pod_image_path, updated_at)
                VALUES
                    (:rid, :inv, :mn, 'PENDING', :path, CURRENT_TIMESTAMP)
                ON CONFLICT (report_item_id) DO UPDATE SET
                    pod_image_path = EXCLUDED.pod_image_path,
                    updated_at     = CURRENT_TIMESTAMP
            """),
            {
                "rid":  report_item_id,
                "inv":  item_row.invoice_number,
                "mn":   item_row.manifest_number,
                "path": rel_path,
            },
        )

        du_row = db.execute(
            text("SELECT * FROM delivery_updates WHERE report_item_id = :id"),
            {"id": report_item_id},
        ).fetchone()

        # Audit trail
        db.execute(
            text("""
                INSERT INTO delivery_events
                    (delivery_update_id, report_item_id, manifest_number, invoice_number,
                     status, pod_image_path, changed_by_user_id, changed_by_username, event_at)
                VALUES
                    (:du_id, :rid, :mn, :inv,
                     :status, :pod_path, :user_id, :username, CURRENT_TIMESTAMP)
            """),
            {
                "du_id":    du_row.id,
                "rid":      report_item_id,
                "mn":       item_row.manifest_number,
                "inv":      item_row.invoice_number,
                "status":   du_row.status,
                "pod_path": rel_path,
                "user_id":  current_user["id"],
                "username": current_user["username"],
            },
        )

        db.commit()

    except HTTPException:
        raise
    except Exception:
        db.rollback()
        logger.error(
            f"DB error during PoD upload for report_item_id={report_item_id}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Internal server error")

    # ── Write to disk (after DB commit) ────────────────────────────────────
    try:
        with open(abs_path, 'wb') as f:
            f.write(header + remainder)
    except OSError:
        # Revert the DB path since the file never landed on disk
        try:
            db.execute(
                text("""
                    UPDATE delivery_updates
                    SET pod_image_path = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE report_item_id = :id
                """),
                {"id": report_item_id},
            )
            db.commit()
        except Exception:
            db.rollback()
            logger.error("Failed to revert pod_image_path after disk write failure", exc_info=True)
        logger.critical(
            f"Disk write failed for PoD report_item_id={report_item_id}: path={abs_path}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="File storage failed")

    logger.info(
        f"PoD uploaded: item={report_item_id} manifest={item_row.manifest_number} "
        f"file={filename} by '{current_user['username']}' (role={role})"
    )

    return PodUploadResponse(
        report_item_id = report_item_id,
        invoice_number = item_row.invoice_number,
        pod_image_path = rel_path,
        has_pod        = True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/delivery/files/{file_path:path}
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/files/{file_path:path}")
def serve_pod_file(
    file_path:    str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """
    Serve an uploaded PoD or signature file.

    Auth    : any authenticated user (all roles).
    Security:
      - Path traversal is blocked — the resolved absolute path must start
        with UPLOADS_ROOT. Any request that escapes the directory is rejected 400.
      - DRIVER role: may only access files stored under a manifest assigned to them.
        The manifest_number is extracted from the path prefix (pods/{manifest_number}/...).
      - ADMIN / DISPATCH / REPORTS_ONLY: unrestricted access to all files.

    file_path : relative path as stored in delivery_updates.pod_image_path
                e.g. pods/MAN-001/20240315_INV1234_doc.jpg
    """
    role = current_user.get("role")

    # DRIVER: enforce manifest ownership before touching the filesystem
    if role == "DRIVER":
        # Expected path shape: pods/{manifest_number}/{filename}
        parts = file_path.replace("\\", "/").split("/")
        if len(parts) < 3 or parts[0] != "pods":
            raise HTTPException(status_code=403, detail="Access denied")
        manifest_number = parts[1]
        if not _is_manifest_assigned_to_driver(db, manifest_number, current_user):
            raise HTTPException(status_code=403, detail="Access denied: file not from your manifest")

    uploads_root = os.path.realpath(settings.UPLOADS_ROOT)
    requested    = os.path.realpath(os.path.join(uploads_root, file_path))

    # Block any path that escapes the uploads root
    if not requested.startswith(uploads_root + os.sep) and requested != uploads_root:
        raise HTTPException(status_code=400, detail="Invalid file path")

    if not os.path.isfile(requested):
        raise HTTPException(status_code=404, detail="File not found")

    # Detect MIME type from magic bytes (not extension) for correct Content-Type
    with open(requested, 'rb') as f:
        file_header = f.read(8)
    _, mime = _detect_file_type(file_header)
    media_type = mime or 'application/octet-stream'

    return FileResponse(requested, media_type=media_type)

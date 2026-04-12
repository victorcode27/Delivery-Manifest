"""
app/routes/tracking.py

v1 truck location tracking endpoints.

Routes
------
  POST /api/tracking/ping                     — driver phone sends a GPS position
  GET  /api/tracking/latest/{manifest_number} — office client fetches the latest known position

Auth matrix
-----------
  POST /ping:
    DRIVER       — only permitted role; driver_username is extracted from the JWT, not the body
    ADMIN        — blocked (403) via require_driver
    DISPATCH     — blocked (403) via require_driver
    REPORTS_ONLY — blocked (403) via require_driver

  GET /latest/{manifest_number}:
    ADMIN        — permitted
    DISPATCH     — permitted
    REPORTS_ONLY — permitted (read-only office view)
    DRIVER       — blocked (403) via require_office_read

Timestamp semantics
-------------------
  recorded_at      — server-assigned TIMESTAMPTZ set by the DB at INSERT time.
                     This is the authoritative timestamp for display, ordering,
                     and staleness detection in v1.  It is never touched by the client.
  device_timestamp — optional ISO-8601 string sent by the device.  Stored verbatim
                     for clock-skew auditing only.  Never used for ordering or display
                     in v1.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from delivery_manifest_backend.app.core.deps import require_driver, require_office_read
from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.db.database import get_db
from delivery_manifest_backend.app.schemas.tracking import (
    LocationPingAck,
    LocationPingIn,
    LocationPingOut,
)

router = APIRouter(prefix="/tracking", tags=["tracking"])
logger = get_logger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# POST /api/tracking/ping
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/ping", response_model=LocationPingAck, status_code=201)
def receive_ping(
    body:         LocationPingIn,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(require_driver),
):
    """
    Accept a GPS position ping from the driver's mobile app.

    driver_username is sourced from the validated JWT (current_user["username"]),
    not from the request body — the client cannot spoof the sending identity.

    manifest_number and reg_number are trusted as provided by the client and stored
    as-is.  No cross-check against the reports table is performed: pings may legally
    arrive before manifest status has been updated server-side.  A mismatch would
    only waste a small amount of storage, not cause a security issue.

    recorded_at is set by the DB at INSERT time and is the authoritative timestamp.
    device_timestamp (if provided) is stored for auditing only and is never used
    for ordering or display.
    """
    try:
        db.execute(
            text("""
                INSERT INTO location_pings
                    (manifest_number, reg_number, driver_username,
                     latitude, longitude, accuracy, device_timestamp)
                VALUES
                    (:manifest_number, :reg_number, :driver_username,
                     :latitude, :longitude, :accuracy, :device_timestamp)
            """),
            {
                "manifest_number":  body.manifest_number,
                "reg_number":       body.reg_number,
                "driver_username":  current_user["username"],
                "latitude":         body.latitude,
                "longitude":        body.longitude,
                "accuracy":         body.accuracy,
                "device_timestamp": body.device_timestamp,
            },
        )
        # Fetch the inserted row by the highest id for this (manifest, driver) pair.
        # Matches the fetch-after-insert pattern used throughout delivery.py.
        # Using id DESC rather than recorded_at DESC avoids any sub-millisecond
        # clock collision on a heavily loaded DB.
        row = db.execute(
            text("""
                SELECT id, recorded_at
                FROM   location_pings
                WHERE  manifest_number = :mn
                  AND  driver_username = :du
                ORDER  BY id DESC
                LIMIT  1
            """),
            {"mn": body.manifest_number, "du": current_user["username"]},
        ).fetchone()
        db.commit()

    except Exception:
        db.rollback()
        logger.error(
            f"Error inserting location ping for manifest='{body.manifest_number}' "
            f"driver='{current_user['username']}'",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Internal server error")

    logger.info(
        f"Ping received: manifest={body.manifest_number} reg={body.reg_number} "
        f"driver='{current_user['username']}' "
        f"lat={body.latitude} lng={body.longitude} accuracy={body.accuracy}"
    )

    return LocationPingAck(id=row.id, recorded_at=str(row.recorded_at))


# ══════════════════════════════════════════════════════════════════════════════
# GET /api/tracking/latest/{manifest_number}
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/latest/{manifest_number}", response_model=LocationPingOut)
def get_latest_ping(
    manifest_number: str,
    db:              Session = Depends(get_db),
    current_user:    dict    = Depends(require_office_read),
):
    """
    Return the most recent GPS ping for a manifest.

    Used by the web office client to display the truck's last known position.
    "Most recent" is defined by recorded_at DESC — the server-assigned timestamp.
    device_timestamp from the client is never used for ordering.

    Returns 404 if no pings have been received for the given manifest yet.

    DRIVER role is blocked via require_office_read — drivers cannot query
    other drivers' positions.
    """
    try:
        row = db.execute(
            text("""
                SELECT id, manifest_number, reg_number, driver_username,
                       latitude, longitude, accuracy, recorded_at, device_timestamp
                FROM   location_pings
                WHERE  manifest_number = :mn
                ORDER  BY recorded_at DESC
                LIMIT  1
            """),
            {"mn": manifest_number},
        ).fetchone()

    except Exception:
        logger.error(
            f"Error fetching latest ping for manifest='{manifest_number}'",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Internal server error")

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No location data found for manifest '{manifest_number}'",
        )

    logger.info(
        f"Latest ping fetched: manifest={manifest_number} "
        f"by '{current_user['username']}' (role={current_user.get('role')})"
    )

    return LocationPingOut(
        id               = row.id,
        manifest_number  = row.manifest_number,
        reg_number       = row.reg_number,
        driver_username  = row.driver_username,
        latitude         = row.latitude,
        longitude        = row.longitude,
        accuracy         = row.accuracy,
        recorded_at      = str(row.recorded_at),
        device_timestamp = row.device_timestamp,
    )

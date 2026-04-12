"""
app/schemas/tracking.py

Pydantic request/response models for the v1 truck location tracking API.
"""

from typing import Optional
from pydantic import BaseModel, field_validator


class LocationPingIn(BaseModel):
    """Body for POST /api/tracking/ping — sent by the driver's mobile app."""
    manifest_number:  str
    reg_number:       str
    latitude:         float
    longitude:        float
    accuracy:         Optional[float] = None   # metres; nullable — device may not report it
    device_timestamp: Optional[str]   = None   # ISO-8601 string from device clock.
                                               # Audit-only in v1: stored verbatim, never used
                                               # for ordering or display.  Use recorded_at for
                                               # all display and staleness logic.

    @field_validator("latitude")
    @classmethod
    def check_latitude(cls, v: float) -> float:
        if not -90.0 <= v <= 90.0:
            raise ValueError(f"latitude must be between -90 and 90, got {v}")
        return v

    @field_validator("longitude")
    @classmethod
    def check_longitude(cls, v: float) -> float:
        if not -180.0 <= v <= 180.0:
            raise ValueError(f"longitude must be between -180 and 180, got {v}")
        return v


class LocationPingAck(BaseModel):
    """Minimal acknowledgment returned after a successful ping insert."""
    id:          int
    recorded_at: str


class LocationPingOut(BaseModel):
    """
    Latest ping for a manifest — returned to the web office client.

    recorded_at is the authoritative timestamp for display, marker placement,
    and staleness detection.  It is server-assigned at INSERT time and is never
    influenced by the client.

    device_timestamp is the client-reported time, stored for clock-skew auditing
    only.  Web clients must NOT use device_timestamp for display or ordering in v1.
    """
    id:               int
    manifest_number:  str
    reg_number:       str
    driver_username:  str
    latitude:         float
    longitude:        float
    accuracy:         Optional[float] = None
    recorded_at:      str             # source of truth for display and staleness
    device_timestamp: Optional[str]   = None   # audit-only; do not use for ordering

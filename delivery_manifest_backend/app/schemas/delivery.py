"""
app/schemas/delivery.py

Pydantic request/response models for the delivery execution API (MVP).

MVP scope: delivery status + notes only.
File uploads (PoD, signature) are placeholders — fields exist in the schema
so the response shape is stable for future phases; they always return False/None.
"""

from typing import List, Optional
from pydantic import BaseModel, field_validator

# Canonical invoice-level delivery statuses (mirrors delivery_updates.status CHECK constraint)
VALID_DELIVERY_STATUSES = (
    "PENDING",
    "IN_TRANSIT",
    "DELIVERED",
    "FAILED",
    "PARTIAL",
    "RETURNED",
)


# ── Nested summary model ───────────────────────────────────────────────────────

class DeliveryStatusSummary(BaseModel):
    """Per-manifest delivery progress counts + derived overall status."""
    status:     str  # PENDING | IN_PROGRESS | COMPLETED | COMPLETED_WITH_ISSUES
    delivered:  int
    pending:    int
    failed:     int
    partial:    int
    returned:   int
    in_transit: int


# ── List endpoint models ───────────────────────────────────────────────────────

class DeliveryManifestSummary(BaseModel):
    """One row in the manifest list — header info + aggregated delivery counts."""
    manifest_number: str
    date_dispatched: Optional[str] = None
    driver:          Optional[str] = None
    reg_number:      Optional[str] = None
    total_items:     int
    delivery_summary: DeliveryStatusSummary


class DeliveryManifestListResponse(BaseModel):
    manifests: List[DeliveryManifestSummary]
    total:     int


# ── Detail endpoint models ─────────────────────────────────────────────────────

class DeliveryManifestItem(BaseModel):
    """One invoice row inside a manifest detail response."""
    report_item_id:  int
    invoice_number:  str
    customer_name:   Optional[str]   = None
    customer_number: Optional[str]   = None
    area:            Optional[str]   = None
    value:           Optional[float] = None
    delivery_status: str
    notes:           Optional[str]   = None
    has_pod:         bool            = False   # always False in MVP
    has_signature:   bool            = False   # always False in MVP
    updated_at:      Optional[str]   = None


class DeliveryManifestDetailResponse(BaseModel):
    manifest_number: str
    driver:          Optional[str] = None
    date_dispatched: Optional[str] = None
    manifest_status: str           # derived, not stored
    items:           List[DeliveryManifestItem]


# ── Update endpoint models ─────────────────────────────────────────────────────

class DeliveryUpdateRequest(BaseModel):
    """Body for PUT /api/delivery/updates/{report_item_id}."""
    status: str
    notes:  Optional[str] = None

    @field_validator("status")
    @classmethod
    def check_status(cls, v: str) -> str:
        if v not in VALID_DELIVERY_STATUSES:
            raise ValueError(
                f"Invalid status '{v}'. Must be one of: {', '.join(VALID_DELIVERY_STATUSES)}"
            )
        return v


class DeliveryUpdateResponse(BaseModel):
    """Current state of a delivery_updates row, returned after every PUT."""
    id:              int
    report_item_id:  int
    invoice_number:  str
    manifest_number: str
    driver_user_id:  Optional[int] = None
    driver_name:     Optional[str] = None
    status:          str
    notes:           Optional[str] = None
    updated_at:      Optional[str] = None

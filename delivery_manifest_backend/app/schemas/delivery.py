"""
app/schemas/delivery.py

Pydantic request/response models for the delivery execution API.
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

# Canonical transition rules for invoice-level delivery status.
# Key   : current status.
# Value : frozenset of statuses that may follow.
# DELIVERED has an empty frozenset — it is terminal; no outbound transitions are allowed.
# PENDING is the implicit initial state and is not a valid target for explicit transitions.
ALLOWED_TRANSITIONS: dict = {
    "PENDING":    frozenset({"IN_TRANSIT"}),
    "IN_TRANSIT": frozenset({"DELIVERED", "FAILED", "PARTIAL", "RETURNED"}),
    "FAILED":     frozenset({"IN_TRANSIT"}),
    "PARTIAL":    frozenset({"IN_TRANSIT"}),
    "RETURNED":   frozenset({"IN_TRANSIT"}),
    "DELIVERED":  frozenset(),  # terminal — no outbound transitions
}


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
    assistant:       Optional[str] = None
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
    delivery_mode:   str             = "INTERNAL"
    notes:           Optional[str]   = None
    has_pod:         bool            = False
    has_signature:   bool            = False
    pod_image_path:  Optional[str]   = None
    updated_at:      Optional[str]   = None


class DeliveryManifestDetailResponse(BaseModel):
    manifest_number: str
    driver:          Optional[str] = None
    assistant:       Optional[str] = None
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


# ── PoD upload response ────────────────────────────────────────────────────────

class PodUploadResponse(BaseModel):
    """Returned after a successful PoD file upload."""
    report_item_id: int
    invoice_number: str
    pod_image_path: str
    has_pod:        bool = True


# ── Bulk confirm response ──────────────────────────────────────────────────────

class BulkConfirmResponse(BaseModel):
    """Returned after POST /manifests/{manifest_number}/bulk-confirm."""
    manifest_number: str
    updated:         int   # invoices changed to DELIVERED
    skipped:         int   # invoices already resolved — left untouched


# ── Bulk status-update schemas ─────────────────────────────────────────────────

# Target statuses accepted by the generic bulk-status-update endpoint.
# PENDING and IN_TRANSIT are excluded: PENDING items are never auto-advanced,
# and IN_TRANSIT is an intermediate state, not a terminal bulk target.
BULK_UPDATE_TARGET_STATUSES = frozenset({"DELIVERED", "RETURNED", "FAILED"})


class BulkStatusUpdateRequest(BaseModel):
    """Body for POST /manifests/{manifest_number}/bulk-status-update."""
    target_status: str

    @field_validator("target_status")
    @classmethod
    def check_target_status(cls, v: str) -> str:
        if v not in BULK_UPDATE_TARGET_STATUSES:
            raise ValueError(
                f"Invalid bulk target status '{v}'. "
                f"Allowed: {', '.join(sorted(BULK_UPDATE_TARGET_STATUSES))}"
            )
        return v


class BulkStatusUpdateResponse(BaseModel):
    """Returned after POST /manifests/{manifest_number}/bulk-status-update."""
    manifest_number: str
    target_status:   str
    updated:         int   # invoices updated to target_status
    skipped:         int   # invoices ineligible or already at target — left untouched

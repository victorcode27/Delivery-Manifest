"""
app/schemas/manifest.py

Pydantic request / response models for the manifest domain:
uploaded manifests, invoices, reports, settings, trucks, and customer routes.
"""

from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel


# ── Uploaded manifest files ────────────────────────────────────────────────────

class ManifestCreate(BaseModel):
    """Payload when a client uploads / registers a manifest file."""
    file_name:   str
    uploaded_by: Optional[int] = None   # user id; None = unauthenticated upload
    status:      str = "PENDING"        # PENDING | PROCESSING | DONE | ERROR


class ManifestOut(BaseModel):
    """Safe public response for a manifest record."""
    id:          int
    file_name:   str
    uploaded_by: Optional[int] = None
    uploaded_at: Optional[datetime] = None
    status:      str

    class Config:
        from_attributes = True   # build from ORM instance


# ── Invoices ──────────────────────────────────────────────────────────────────

class InvoiceOut(BaseModel):
    """A single invoice as returned by the API."""
    filename:        str
    date_processed:  str
    customer_name:   str
    total_value:     str
    order_number:    str
    invoice_number:  Optional[str] = "N/A"
    customer_number: Optional[str] = "N/A"
    invoice_date:    Optional[str] = "N/A"
    area:            Optional[str] = "UNKNOWN"


class ManualInvoiceRequest(BaseModel):
    """Payload for manually adding an invoice (no PDF)."""
    customer_name:   str
    total_value:     str
    invoice_number:  str
    order_number:    str
    customer_number: Optional[str] = "N/A"
    area:            Optional[str] = "UNKNOWN"


# ── Staging / allocation ───────────────────────────────────────────────────────

class AllocateRequest(BaseModel):
    """Add / remove a batch of invoices (by filename) to/from a manifest."""
    filenames:       List[str]
    manifest_number: Optional[str] = None


# ── Dispatch reports ───────────────────────────────────────────────────────────

class ReportInvoiceItem(BaseModel):
    """One invoice line inside a dispatch report payload."""
    num:          Optional[str]   = None
    invoice_number: Optional[str] = None
    orderNum:     Optional[str]   = None
    order_number: Optional[str]   = None
    customer:     Optional[str]   = None
    customer_name: Optional[str]  = None
    customerNumber: Optional[str] = None
    customer_number: Optional[str]= None
    invoiceDate:  Optional[str]   = None
    invoice_date: Optional[str]   = None
    area:         Optional[str]   = "UNKNOWN"
    sku:          int             = 0
    value:        float           = 0
    total_value:  Optional[float] = 0
    weight:       float           = 0


class ReportRequest(BaseModel):
    """Full payload to save a dispatch report."""
    manifestNumber: str
    date:           str
    driver:         Optional[str]   = None
    assistant:      Optional[str]   = None
    checker:        Optional[str]   = None
    regNumber:      Optional[str]   = None
    palletsBrown:   int             = 0
    palletsBlue:    int             = 0
    crates:         int             = 0
    mileage:        int             = 0
    totalValue:     float           = 0
    totalSku:       int             = 0
    totalWeight:    float           = 0
    invoices:       List[ReportInvoiceItem] = []


# ── Settings ──────────────────────────────────────────────────────────────────

class SettingRequest(BaseModel):
    category: str
    value:    str


class SettingUpdateRequest(BaseModel):
    category:  str
    old_value: str
    new_value: str


# ── Trucks ────────────────────────────────────────────────────────────────────

class TruckRequest(BaseModel):
    reg:       str
    driver:    Optional[str] = None
    assistant: Optional[str] = None
    checker:   Optional[str] = None


# ── Customer routes ───────────────────────────────────────────────────────────

class CustomerRouteRequest(BaseModel):
    customer_name: str
    route_name:    str

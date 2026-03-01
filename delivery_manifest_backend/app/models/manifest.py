"""
app/models/manifest.py

SQLAlchemy ORM models for the delivery manifest domain:

    Manifest       → `manifests` table  (uploaded manifest files)
    Order          → `orders` table  (invoices / credit notes)
    Report         → `reports` table (dispatch run summaries)
    ReportItem     → `report_items` table (per-invoice rows in a report)
    Setting        → `settings` table (drivers, routes, checkers, …)
    Truck          → `trucks` table
    CustomerRoute  → `customer_routes` table
    ManifestEvent  → `manifest_events` table (audit trail)
    ManifestStaging→ `manifest_staging` table (in-progress allocation)
"""

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Index, Integer, String, Text, TIMESTAMP,
)
from sqlalchemy.orm import relationship
from delivery_manifest_backend.app.db.database import Base


# ── Uploaded manifest files ────────────────────────────────────────────────────

class Manifest(Base):
    __tablename__ = "manifests"

    id          = Column(Integer, primary_key=True, index=True)
    file_name   = Column(Text, nullable=False)
    uploaded_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    uploaded_at = Column(DateTime)
    status      = Column(Text, default="PENDING")  # PENDING | PROCESSING | DONE | ERROR

    uploader = relationship("User", back_populates="manifests")

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# ── Orders (invoices / credit notes) ──────────────────────────────────────────

class Order(Base):
    __tablename__ = "orders"

    id               = Column(Integer, primary_key=True, index=True)
    filename         = Column(Text, unique=True, nullable=False)
    date_processed   = Column(Text, nullable=False)
    customer_name    = Column(Text, nullable=False)
    total_value      = Column(Text, default="0.00")
    order_number     = Column(Text, default="N/A")
    invoice_number   = Column(Text, default="N/A")
    invoice_date     = Column(Text, default="N/A")
    area             = Column(Text, default="UNKNOWN")
    is_allocated     = Column(Integer, default=0)
    allocated_date   = Column(Text)
    manifest_number  = Column(Text)
    type             = Column(Text, default="INVOICE")   # INVOICE | CREDIT_NOTE
    reference_number = Column(Text)
    original_value   = Column(Text)
    status           = Column(Text, default="PENDING")   # PENDING | CANCELLED
    customer_number  = Column(Text, default="N/A")

    staging_entries  = relationship("ManifestStaging", back_populates="order")

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# ── Dispatch reports ───────────────────────────────────────────────────────────

class Report(Base):
    __tablename__ = "reports"

    id              = Column(Integer, primary_key=True, index=True)
    manifest_number = Column(Text, nullable=False, index=True)
    date            = Column(Text)
    date_dispatched = Column(Text, index=True)
    driver          = Column(Text)
    assistant       = Column(Text)
    checker         = Column(Text)
    reg_number      = Column(Text)
    pallets_brown   = Column(Integer, default=0)
    pallets_blue    = Column(Integer, default=0)
    crates          = Column(Integer, default=0)
    mileage         = Column(Integer, default=0)
    total_value     = Column(Float, default=0)
    total_sku       = Column(Integer, default=0)
    total_weight    = Column(Float, default=0)
    session_id      = Column(Text)
    created_at      = Column(Text)

    items = relationship("ReportItem", back_populates="report", cascade="all, delete-orphan")

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# ── Per-invoice rows inside a report ──────────────────────────────────────────

class ReportItem(Base):
    __tablename__ = "report_items"

    id              = Column(Integer, primary_key=True, index=True)
    report_id       = Column(Integer, ForeignKey("reports.id"), nullable=False)
    invoice_number  = Column(Text, nullable=False)
    order_number    = Column(Text)
    customer_name   = Column(Text)
    customer_number = Column(Text)
    invoice_date    = Column(Text)
    area            = Column(Text)
    sku             = Column(Integer, default=0)
    value           = Column(Float, default=0)
    weight          = Column(Float, default=0)

    report = relationship("Report", back_populates="items")

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# ── App settings (drivers, routes, checkers, …) ───────────────────────────────

class Setting(Base):
    __tablename__ = "settings"

    id       = Column(Integer, primary_key=True, index=True)
    category = Column(Text, nullable=False)
    value    = Column(Text, nullable=False)

    def to_dict(self) -> dict:
        return {"id": self.id, "category": self.category, "value": self.value}


# ── Fleet trucks ──────────────────────────────────────────────────────────────

class Truck(Base):
    __tablename__ = "trucks"

    id        = Column(Integer, primary_key=True, index=True)
    reg       = Column(Text, unique=True, nullable=False)
    driver    = Column(Text)
    assistant = Column(Text)
    checker   = Column(Text)

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# ── Customer → route mappings ─────────────────────────────────────────────────

class CustomerRoute(Base):
    __tablename__ = "customer_routes"

    id            = Column(Integer, primary_key=True, index=True)
    customer_name = Column(Text, unique=True, nullable=False)
    route_name    = Column(Text, nullable=False)

    def to_dict(self) -> dict:
        return {"customer_name": self.customer_name, "route_name": self.route_name}


# ── Manifest audit trail ───────────────────────────────────────────────────────

class ManifestEvent(Base):
    __tablename__ = "manifest_events"

    id              = Column(Integer, primary_key=True, index=True)
    manifest_number = Column(Text, nullable=False, index=True)
    event_type      = Column(Text, nullable=False)
    performed_by    = Column(Text, default="System")
    timestamp       = Column(Text, nullable=False)

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# ── In-progress manifest staging ──────────────────────────────────────────────

class ManifestStaging(Base):
    __tablename__ = "manifest_staging"

    id         = Column(Integer, primary_key=True, index=True)
    session_id = Column(Text, nullable=False, index=True)
    invoice_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    added_at   = Column(TIMESTAMP)

    order = relationship("Order", back_populates="staging_entries")

    def to_dict(self) -> dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

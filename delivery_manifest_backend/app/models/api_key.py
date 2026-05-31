"""
app/models/api_key.py

SQLAlchemy ORM model for the ``api_keys`` table.

Each row represents a static API key that an external agent or service can
use to authenticate without handling JWT token refresh.

Column notes
------------
- ``key_prefix``   First 8 characters of the raw key — shown in admin UI so
                   the owner can identify which key is which.
- ``key_hash``     bcrypt hash of the full raw key — never stored in plaintext.
- ``name``         Human-readable label, e.g. "Automation Agent", "Reporting Bot".
- ``role``         Same role constants as users: ADMIN | DISPATCH | REPORTS_ONLY | DRIVER.
- ``is_active``    Can be set to False to revoke without deleting.
- ``created_by``   FK to the admin user who created the key.
- ``last_used_at`` Updated every time the key is used successfully.
- ``expires_at``   Optional expiry. NULL = never expires.
"""

from sqlalchemy import Boolean, Column, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.sql import func

from delivery_manifest_backend.app.db.database import Base


class ApiKey(Base):
    __tablename__ = "api_keys"

    id            = Column(Integer, primary_key=True, index=True)
    name          = Column(Text, nullable=False)
    key_prefix    = Column(Text, nullable=False)           # first 8 chars (display only)
    key_hash      = Column(Text, nullable=False)           # bcrypt hash of full key
    role          = Column(Text, nullable=False, default="DISPATCH")
    is_active     = Column(Boolean, default=True, nullable=False)
    created_by    = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at    = Column(TIMESTAMP(timezone=True), server_default=func.now())
    last_used_at  = Column(TIMESTAMP(timezone=True), nullable=True)
    expires_at    = Column(TIMESTAMP(timezone=True), nullable=True)

    def to_dict(self) -> dict:
        """Serialise to a plain dict (key_hash is NEVER included)."""
        return {
            "id":           self.id,
            "name":         self.name,
            "key_prefix":   self.key_prefix,
            "role":         self.role,
            "is_active":    self.is_active,
            "created_by":   self.created_by,
            "created_at":   self.created_at.isoformat() if self.created_at else None,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "expires_at":   self.expires_at.isoformat() if self.expires_at else None,
        }

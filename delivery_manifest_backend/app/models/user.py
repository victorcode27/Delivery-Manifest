"""
app/models/user.py

SQLAlchemy ORM model for the ``users`` table.

Column notes
------------
- ``hashed_password``  maps to the DB column ``password_hash`` (legacy name kept
  so existing rows are not broken; SQLAlchemy ``key=`` handles the rename).
- ``role``             'FULL_ACCESS' | 'REPORTS_ONLY'
- ``is_active``        account enabled / disabled
- ``created_at``       TIMESTAMPTZ, set on insert
- ``updated_at``       TIMESTAMPTZ, set on insert and update

Legacy columns ``is_admin`` and ``can_manifest`` are **not** mapped here.
They remain in the DB for rollback safety but are unused by new code.
"""

from sqlalchemy import Boolean, Column, Integer, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from delivery_manifest_backend.app.db.database import Base


class User(Base):
    __tablename__ = "users"

    id              = Column(Integer, primary_key=True, index=True)
    username        = Column(Text, unique=True, nullable=False, index=True)

    # DB column is still called ``password_hash``; Python attribute is ``hashed_password``
    hashed_password = Column("password_hash", Text, nullable=False)

    role            = Column(Text, nullable=False, default="FULL_ACCESS")
    is_active       = Column(Boolean, default=True)
    created_at      = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at      = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

    manifests = relationship("Manifest", back_populates="uploader")

    # ── Convenience helpers ───────────────────────────────────────────────────

    @property
    def has_full_access(self) -> bool:
        """Check whether this user has full system access."""
        return self.role == "FULL_ACCESS"

    def to_dict(self) -> dict:
        """Serialise to a plain dict (password hash is **never** included)."""
        return {
            "id":         self.id,
            "username":   self.username,
            "role":       self.role,
            "is_active":  self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

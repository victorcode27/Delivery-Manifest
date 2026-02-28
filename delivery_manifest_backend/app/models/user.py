"""
app/models/user.py

SQLAlchemy ORM model for the `users` table.

Column notes
------------
- `hashed_password`  maps to the DB column `password_hash` (legacy name kept
  so existing rows are not broken; SQLAlchemy `key=` handles the rename).
- `is_active`        replaces the old `can_manifest` flag — same default (1/True).
- `role`             replaces the old `is_admin` flag — "admin" | "user".
"""

from sqlalchemy import Boolean, Column, Integer, String, Text
from sqlalchemy.orm import relationship

from app.db.database import Base


class User(Base):
    __tablename__ = "users"

    id              = Column(Integer, primary_key=True, index=True)
    username        = Column(Text, unique=True, nullable=False, index=True)

    # DB column is still called `password_hash`; Python attribute is `hashed_password`
    hashed_password = Column("password_hash", Text, nullable=False)

    is_active       = Column(Boolean, default=True)
    role            = Column(Text, default="user")   # "admin" | "user"
    created_at      = Column(Text)

    manifests = relationship("Manifest", back_populates="uploader")

    # ── Convenience helpers ───────────────────────────────────────────────────

    @property
    def is_admin(self) -> bool:
        """Backward-compatible check used by existing service code."""
        return self.role == "admin"

    def to_dict(self, include_password: bool = False) -> dict:
        """Serialise to a plain dict (password hash excluded by default)."""
        data = {
            "id":         self.id,
            "username":   self.username,
            "is_active":  self.is_active,
            "role":       self.role,
            "created_at": self.created_at,
        }
        if include_password:
            data["hashed_password"] = self.hashed_password
        return data

"""
app/schemas/api_key.py

Pydantic schemas for API key creation, listing, and responses.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ApiKeyCreate(BaseModel):
    """Body for POST /api/api-keys — admin creates a new key."""
    name: str = Field(..., min_length=1, max_length=100,
                      description="Human-readable label for this key, e.g. 'Automation Agent'")
    role: str = Field(default="DISPATCH",
                      description="Role this key acts as: ADMIN | DISPATCH | REPORTS_ONLY | DRIVER")
    expires_at: Optional[datetime] = Field(
        default=None,
        description="Optional ISO-8601 expiry datetime. Omit or null = never expires."
    )


class ApiKeyResponse(BaseModel):
    """Returned after a key is created — includes the raw key ONCE."""
    id:          int
    name:        str
    key_prefix:  str
    role:        str
    is_active:   bool
    created_at:  Optional[datetime]
    expires_at:  Optional[datetime]
    raw_key:     str   # returned ONLY on creation; never stored, never returned again


class ApiKeyListItem(BaseModel):
    """Safe summary returned by GET /api/api-keys (no raw key, no hash)."""
    id:           int
    name:         str
    key_prefix:   str
    role:         str
    is_active:    bool
    created_by:   Optional[int]
    created_at:   Optional[datetime]
    last_used_at: Optional[datetime]
    expires_at:   Optional[datetime]


class ApiKeyRevokeResponse(BaseModel):
    message: str

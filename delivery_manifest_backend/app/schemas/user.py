"""
app/schemas/user.py

Pydantic request / response models for user management and authentication.
"""

from typing import Optional
from pydantic import BaseModel, Field, field_validator

from delivery_manifest_backend.app.core.security import (
    VALID_ROLES,
    validate_password_strength,
)


# ── Auth ──────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str = Field(max_length=128)


class LoginResponse(BaseModel):
    success:      bool
    access_token: Optional[str] = None
    token_type:   str = "bearer"
    user:         "UserOut"


# ── User CRUD ─────────────────────────────────────────────────────────────────
# VALID_ROLES is the canonical tuple defined in app.core.security — do not
# redefine it here.

class UserCreate(BaseModel):
    """Payload to register a new user."""
    username:  str
    password:  str  = Field(max_length=128)
    role:      str  = "REPORTS_ONLY"
    is_active: bool = True

    @field_validator("password")
    @classmethod
    def check_password_strength(cls, v: str) -> str:
        errors = validate_password_strength(v)
        if errors:
            raise ValueError("; ".join(errors))
        return v

    @field_validator("role")
    @classmethod
    def check_role(cls, v: str) -> str:
        if v not in VALID_ROLES:
            raise ValueError(f"Role must be one of: {', '.join(VALID_ROLES)}")
        return v


class PasswordReset(BaseModel):
    """Payload to reset a user's password."""
    password: str = Field(max_length=128)

    @field_validator("password")
    @classmethod
    def check_password_strength(cls, v: str) -> str:
        errors = validate_password_strength(v)
        if errors:
            raise ValueError("; ".join(errors))
        return v


class RoleUpdate(BaseModel):
    """Payload to change a user's access level."""
    role: str

    @field_validator("role")
    @classmethod
    def check_role(cls, v: str) -> str:
        if v not in VALID_ROLES:
            raise ValueError(f"Role must be one of: {', '.join(VALID_ROLES)}")
        return v


class StatusUpdate(BaseModel):
    """Payload to activate / deactivate a user."""
    is_active: bool


class UserOut(BaseModel):
    """Safe public representation — password hash is never included."""
    id:         int
    username:   str
    role:       str
    is_active:  bool
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        from_attributes = True

"""
app/schemas/user.py

Pydantic request / response models for user management and authentication.
"""

from typing import Optional
from pydantic import BaseModel, Field


# ── Auth ──────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    success:      bool
    access_token: Optional[str] = None
    token_type:   str = "bearer"
    user:         "UserOut"


# ── User CRUD ─────────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    """Payload to register a new user."""
    username:  str
    password:  str
    is_active: bool = True
    role:      str  = "user"   # "admin" | "user"


class UserUpdate(BaseModel):
    """All fields optional — only supplied fields are updated."""
    password:  Optional[str]  = None
    is_active: Optional[bool] = None
    role:      Optional[str]  = None


class UserOut(BaseModel):
    """Safe public representation — password hash is never included."""
    id:         int
    username:   str
    is_active:  bool = True
    role:       str  = "user"
    created_at: Optional[str] = None

    # camelCase aliases so the React / mobile frontend keeps working
    isActive: Optional[bool] = Field(default=None, alias="isActive")
    isAdmin:  Optional[bool] = Field(default=None, alias="isAdmin")

    class Config:
        populate_by_name = True   # accept both snake_case and camelCase
        from_attributes  = True   # allow building from ORM instance

"""
app/core/security.py

Password hashing (bcrypt via passlib) and JWT utilities (python-jose).

All token logic lives here.  FastAPI dependency helpers that need DB access
live in app/core/deps.py to avoid circular imports.
"""

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt
from passlib.context import CryptContext

from delivery_manifest_backend.app.core.config import settings

# ── Password helpers ──────────────────────────────────────────────────────────

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_password_hash(password: str) -> str:
    """Return a bcrypt hash of *password*."""
    return _pwd_context.hash(password)


def verify_password(plain_password: str, stored_hash: str) -> bool:
    """
    Verify *plain_password* against *stored_hash*.

    Handles both new-style bcrypt hashes (start with ``$2b$``) and the
    legacy SHA-256 hex strings written by the original flat-file backend.
    This lets existing users log in without a forced password reset.
    """
    if stored_hash.startswith("$2"):
        return _pwd_context.verify(plain_password, stored_hash)
    # Legacy SHA-256 fallback
    return hashlib.sha256(plain_password.encode()).hexdigest() == stored_hash


# Backward-compatible alias — user_service.py and database.py call hash_password()
hash_password = get_password_hash


# ── JWT helpers ───────────────────────────────────────────────────────────────

_DEFAULT_EXPIRY = timedelta(hours=8)


def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    Create a signed HS256 JWT.

    Usage::

        token = create_access_token({"sub": user.username, "role": user.role})

    Returns a compact token string ready to send in an Authorization header.
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or _DEFAULT_EXPIRY)
    to_encode["exp"] = expire
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_access_token(token: str) -> Optional[dict]:
    """
    Decode and verify a JWT.

    Returns the payload dict on success, or ``None`` if the token is
    invalid, expired, or tampered with.
    """
    try:
        return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError:
        return None

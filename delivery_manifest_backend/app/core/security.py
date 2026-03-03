"""
app/core/security.py

Password hashing (bcrypt direct — passlib removed due to incompatibility
with bcrypt 4.x on Python 3.14) and JWT utilities (python-jose).

All token logic lives here.  FastAPI dependency helpers that need DB access
live in app/core/deps.py to avoid circular imports.
"""

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt as _bcrypt_lib
from jose import JWTError, jwt

from delivery_manifest_backend.app.core.config import settings

# ── Password helpers ──────────────────────────────────────────────────────────

# bcrypt's hard limit is 72 bytes.  bcrypt 4.x raises ValueError for longer
# inputs.  We truncate at the byte level so every call is safe.
_BCRYPT_MAX_BYTES = 72


def _to_bcrypt_bytes(password: str) -> bytes:
    """Encode *password* to UTF-8 and cap at 72 bytes."""
    return password.encode("utf-8")[:_BCRYPT_MAX_BYTES]


def get_password_hash(password: str) -> str:
    """Return a bcrypt hash of *password* (capped at 72 bytes)."""
    return _bcrypt_lib.hashpw(_to_bcrypt_bytes(password), _bcrypt_lib.gensalt()).decode("utf-8")


def verify_password(plain_password: str, stored_hash: str) -> bool:
    """
    Verify *plain_password* against *stored_hash*.

    Handles both new-style bcrypt hashes (start with ``$2``) and the
    legacy SHA-256 hex strings written by the original flat-file backend.
    This lets existing users log in without a forced password reset.
    """
    if stored_hash.startswith("$2"):
        return _bcrypt_lib.checkpw(
            _to_bcrypt_bytes(plain_password),
            stored_hash.encode("utf-8"),
        )
    # Legacy SHA-256 fallback — SHA-256 has no length limit.
    return hashlib.sha256(plain_password.encode()).hexdigest() == stored_hash


# Backward-compatible alias — user_service.py and database.py call hash_password()
hash_password = get_password_hash


# ── Password policy ───────────────────────────────────────────────────────

def validate_password_strength(password: str) -> list[str]:
    """
    Validate *password* against the system password policy.

    Returns a list of human-readable failure descriptions.
    An empty list means the password is acceptable.

    Policy:
      • Minimum 10 characters
      • At least one uppercase letter
      • At least one lowercase letter
      • At least one digit
    """
    errors: list[str] = []
    if len(password) < 10:
        errors.append("Minimum 10 characters required")
    if not any(c.isupper() for c in password):
        errors.append("At least one uppercase letter required")
    if not any(c.islower() for c in password):
        errors.append("At least one lowercase letter required")
    if not any(c.isdigit() for c in password):
        errors.append("At least one number required")
    return errors


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

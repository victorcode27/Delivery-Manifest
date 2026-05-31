"""
app/core/security.py

Password hashing (bcrypt direct — passlib removed due to incompatibility
with bcrypt 4.x on Python 3.14) and JWT utilities (python-jose).

All token logic lives here.  FastAPI dependency helpers that need DB access
live in app/core/deps.py to avoid circular imports.
"""

import hashlib
import secrets as _secrets
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


# ── Role constants ────────────────────────────────────────────────────────
# Single canonical definition — imported by schemas/user.py and user_service.py.
# Never define VALID_ROLES anywhere else.

VALID_ROLES: tuple[str, ...] = ("ADMIN", "DISPATCH", "REPORTS_ONLY", "DRIVER")


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


def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    Create a signed HS256 JWT.

    Expiry uses ``expires_delta`` when supplied, otherwise falls back to
    ``settings.ACCESS_TOKEN_EXPIRE_MINUTES`` (configured via the .env file).

    Usage::

        token = create_access_token({"sub": user.username, "role": user.role})

    Returns a compact token string ready to send in an Authorization header.
    """
    to_encode = data.copy()
    if expires_delta is None:
        expires_delta = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    expire = datetime.now(timezone.utc) + expires_delta
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


# ── API key helpers ───────────────────────────────────────────────────────────


def generate_api_key() -> tuple[str, str, str]:
    """
    Generate a new random API key.

    Returns a tuple of (raw_key, key_prefix, key_hash) where:
      - raw_key    is the full key to give to the client once (never stored).
      - key_prefix is the first 8 characters shown in admin UIs.
      - key_hash   is the bcrypt hash stored in the database.

    Key format:  ``dmk_<64 hex chars>``  (dmk = Delivery Manifest Key)
    Total length: 69 characters (well within bcrypt's 72-byte limit).
    """
    token    = _secrets.token_hex(32)          # 64 hex chars = 256 bits of entropy
    raw_key  = f"dmk_{token}"
    prefix   = raw_key[:8]
    key_hash = _bcrypt_lib.hashpw(
        raw_key.encode("utf-8")[:_BCRYPT_MAX_BYTES],
        _bcrypt_lib.gensalt(),
    ).decode("utf-8")
    return raw_key, prefix, key_hash


def verify_api_key(raw_key: str, stored_hash: str) -> bool:
    """
    Verify a raw API key string against its stored bcrypt hash.

    Returns True if they match, False otherwise.
    """
    try:
        return _bcrypt_lib.checkpw(
            raw_key.encode("utf-8")[:_BCRYPT_MAX_BYTES],
            stored_hash.encode("utf-8"),
        )
    except Exception:
        return False

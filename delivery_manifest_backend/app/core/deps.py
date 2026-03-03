"""
app/core/deps.py

FastAPI dependency helpers for JWT authentication.

Usage in a route::

    from app.core.deps import get_current_user, require_full_access

    @router.get("/protected")
    def protected(user: dict = Depends(get_current_user)):
        return {"hello": user["username"]}

    @router.get("/admin-only")
    def admin_only(user: dict = Depends(require_full_access)):
        ...
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import text

from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.core.security import decode_access_token
from delivery_manifest_backend.app.db.database import get_db_session

logger = get_logger(__name__)

# Tells FastAPI / Swagger UI where the token endpoint is (informational only)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """
    Validate the Bearer JWT and return the corresponding user row as a dict.

    Raises:
        401 – token missing, invalid, or expired
        401 – username claim not found in DB
        403 – account is inactive
    """
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )

    payload = decode_access_token(token)
    if not payload:
        logger.warning("Token validation failed: invalid or expired token")
        raise credentials_exc

    username: str = payload.get("sub", "")
    if not username:
        logger.warning("Token missing 'sub' claim")
        raise credentials_exc

    db = get_db_session()
    try:
        result = db.execute(
            text("SELECT * FROM users WHERE username = :u"),
            {"u": username},
        )
        row = result.fetchone()
        user = dict(row._mapping) if row else None
    finally:
        db.close()

    if not user:
        logger.warning(f"Token references unknown user: '{username}'")
        raise credentials_exc

    # is_active may be NULL for rows created before the v2 migration;
    # treat NULL as active so legacy accounts are not accidentally locked out.
    if user.get("is_active") is False:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is inactive",
        )

    return user


def require_full_access(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Extend ``get_current_user`` — raises 403 if the caller does not have
    FULL_ACCESS role.

    Checks the new ``role`` column first; falls back to the legacy
    ``is_admin`` integer column so both old and new rows are handled
    during migration.
    """
    has_access = (
        current_user.get("role") == "FULL_ACCESS"
        or bool(current_user.get("is_admin", 0))
    )
    if not has_access:
        logger.warning(f"Full access denied for user '{current_user.get('username')}'")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Full access required",
        )
    return current_user


# Backward-compatible alias — existing code may reference require_admin
require_admin = require_full_access

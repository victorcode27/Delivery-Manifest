"""
app/core/deps.py

FastAPI dependency helpers for JWT authentication.

Three permission tiers (lowest → highest):

  get_current_user         — any authenticated, active user
  require_dispatch_or_admin — ADMIN or DISPATCH (manifest/invoice/report writes)
  require_admin            — ADMIN only (settings, trucks, users)

Usage in a route::

    from app.core.deps import get_current_user, require_dispatch_or_admin, require_admin

    @router.post("/invoices/allocate")
    def allocate(user: dict = Depends(require_dispatch_or_admin)):
        ...

    @router.post("/settings")
    def add_setting(user: dict = Depends(require_admin)):
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


def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Raises 403 unless the caller has ADMIN role.
    """
    role = current_user.get("role", "")
    if role not in ("ADMIN",):
        logger.warning(
            f"Admin access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return current_user


def require_dispatch_or_admin(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Raises 403 unless the caller has ADMIN or DISPATCH role.

    Covers all manifest / invoice / report write operations that REPORTS_ONLY
    users must not perform.
    """
    role = current_user.get("role", "")
    if role not in ("ADMIN", "DISPATCH"):
        logger.warning(
            f"Dispatch/Admin access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Manifest access required",
        )
    return current_user

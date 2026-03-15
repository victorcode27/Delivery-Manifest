"""
app/core/deps.py

FastAPI dependency helpers for JWT authentication.

Permission tiers (lowest → highest):

  get_current_user          — any authenticated, active user
  require_delivery_access   — DRIVER, ADMIN, or DISPATCH (delivery execution)
  require_dispatch_or_admin — ADMIN or DISPATCH (manifest/invoice/report writes)
  require_office            — ADMIN or DISPATCH (office-side delivery oversight)
  require_admin             — ADMIN only (settings, trucks, users)
  require_driver            — DRIVER only (field-driver-exclusive endpoints)

Usage in a route::

    from app.core.deps import (
        get_current_user, require_dispatch_or_admin, require_admin,
        require_driver, require_office, require_delivery_access,
    )

    @router.post("/invoices/allocate")
    def allocate(user: dict = Depends(require_dispatch_or_admin)):
        ...

    @router.get("/delivery/manifests")
    def list_manifests(user: dict = Depends(require_delivery_access)):
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


def require_driver(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Raises 403 unless the caller has DRIVER role.

    Reserved for endpoints that are exclusively for field drivers.
    """
    role = current_user.get("role", "")
    if role not in ("DRIVER",):
        logger.warning(
            f"Driver access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Driver access required",
        )
    return current_user


def require_office(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Raises 403 unless the caller has ADMIN or DISPATCH role.

    Covers office-side delivery oversight operations (e.g. viewing full audit
    trails) that field drivers must not access.
    """
    role = current_user.get("role", "")
    if role not in ("ADMIN", "DISPATCH"):
        logger.warning(
            f"Office access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Office access required",
        )
    return current_user


def require_office_read(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Raises 403 if the caller has DRIVER role.

    Covers read-only office data (invoices, reports, manifest detail) that
    field drivers must not access.  ADMIN, DISPATCH, and REPORTS_ONLY are
    all permitted — only DRIVER is blocked.
    """
    role = current_user.get("role", "")
    if role == "DRIVER":
        logger.warning(
            f"Office-read access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Office access required",
        )
    return current_user


def require_delivery_access(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Raises 403 unless the caller has DRIVER, ADMIN, or DISPATCH role.

    REPORTS_ONLY users are explicitly excluded — they have no role in the
    delivery execution workflow.
    """
    role = current_user.get("role", "")
    if role not in ("DRIVER", "ADMIN", "DISPATCH"):
        logger.warning(
            f"Delivery access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Delivery access required",
        )
    return current_user


def require_delivery_read(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Raises 403 unless the caller has DRIVER, ADMIN, DISPATCH, or REPORTS_ONLY role.

    Covers read-only delivery data (manifest list, manifest detail).
    All four roles are permitted — REPORTS_ONLY is explicitly included.
    Write endpoints (PUT status, POST PoD) must still use require_delivery_access.
    """
    role = current_user.get("role", "")
    if role not in ("DRIVER", "ADMIN", "DISPATCH", "REPORTS_ONLY"):
        logger.warning(
            f"Delivery read access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Delivery access required",
        )
    return current_user

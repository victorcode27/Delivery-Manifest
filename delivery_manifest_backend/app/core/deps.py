"""
app/core/deps.py

FastAPI dependency helpers for JWT authentication and API key authentication.

Permission tiers (lowest → highest):

  get_current_user          — any authenticated, active user (JWT or API key)
  require_delivery_read     — all four roles (delivery list/detail reads)
  require_delivery_access   — DRIVER, ADMIN, or DISPATCH (delivery execution writes)
  require_office_read       — ADMIN, DISPATCH, REPORTS_ONLY; DRIVER blocked
  require_dispatch_or_admin — ADMIN or DISPATCH (manifest/invoice/report writes)
  require_office            — ADMIN or DISPATCH (office-side delivery oversight)
  require_admin             — ADMIN only (settings, trucks, users)

Authentication priority:
  1. If ``X-API-Key`` header is present → validate against api_keys table.
  2. Otherwise → validate Bearer JWT token as before.

Usage in a route::

    from app.core.deps import (
        get_current_user, require_dispatch_or_admin, require_admin,
        require_office, require_delivery_access,
    )

    @router.post("/invoices/allocate")
    def allocate(user: dict = Depends(require_dispatch_or_admin)):
        ...

    @router.get("/delivery/manifests")
    def list_manifests(user: dict = Depends(require_delivery_access)):
        ...
"""

from datetime import datetime, timezone

from fastapi import Depends, Header, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import text

from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.core.security import decode_access_token, verify_api_key
from delivery_manifest_backend.app.db.database import get_db_session

logger = get_logger(__name__)

# Tells FastAPI / Swagger UI where the token endpoint is (informational only)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)

# ── Named role subsets ────────────────────────────────────────────────────────
# Derived from VALID_ROLES (canonical in security.py).  Guard functions use
# these frozensets so each permission tier has a single named definition.
_ADMIN_ROLES          = frozenset({"ADMIN"})
_OFFICE_ROLES         = frozenset({"ADMIN", "DISPATCH"})
_OFFICE_READ_ROLES    = frozenset({"ADMIN", "DISPATCH", "REPORTS_ONLY"})
_DELIVERY_WRITE_ROLES = frozenset({"DRIVER", "ADMIN", "DISPATCH"})
_DELIVERY_READ_ROLES  = frozenset({"DRIVER", "ADMIN", "DISPATCH", "REPORTS_ONLY"})
_DRIVER_ROLES         = frozenset({"DRIVER"})


def get_current_user(
    token: str = Depends(oauth2_scheme),
    x_api_key: str = Header(default=None, alias="X-API-Key"),
) -> dict:
    """
    Validate credentials and return the corresponding user (or API key) context.

    Authentication priority:
      1. If ``X-API-Key`` header is present → validate against api_keys table.
         Returns a synthetic user dict built from the api_key row.
      2. Otherwise → validate Bearer JWT token.

    Raises:
        401 – no credentials provided, or invalid/expired token/key
        403 – account is inactive or key is revoked/expired
    """
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    # ── Path 1: API Key ───────────────────────────────────────────────────────
    if x_api_key:
        # Use key_prefix (first 8 chars) to narrow to at most one candidate
        # before running the expensive bcrypt check — avoids O(n) bcrypt scans.
        key_prefix = x_api_key[:8]
        db = get_db_session()
        try:
            result = db.execute(
                text("""
                    SELECT id, name, key_hash, role, is_active,
                           created_by, expires_at
                    FROM api_keys
                    WHERE key_prefix = :prefix AND is_active = TRUE
                """),
                {"prefix": key_prefix},
            )
            rows = result.fetchall()
        finally:
            db.close()

        matched = None
        for row in rows:
            row_dict = dict(row._mapping)
            if verify_api_key(x_api_key, row_dict["key_hash"]):
                matched = row_dict
                break

        if not matched:
            logger.warning("API key authentication failed — no matching active key")
            raise credentials_exc

        if not matched["is_active"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="API key is revoked",
            )

        # Check optional expiry
        if matched["expires_at"] is not None:
            exp = matched["expires_at"]
            if hasattr(exp, "tzinfo") and exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > exp:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="API key has expired",
                )

        # Update last_used_at in background (best-effort, no crash on failure)
        try:
            db2 = get_db_session()
            try:
                db2.execute(
                    text("UPDATE api_keys SET last_used_at = NOW() WHERE id = :id"),
                    {"id": matched["id"]},
                )
                db2.commit()
            finally:
                db2.close()
        except Exception:
            pass

        logger.info(f"API key authenticated: '{matched['name']}' (role={matched['role']})")

        # Return a synthetic user dict that looks like a normal user row
        return {
            "id":         matched["created_by"] or 0,
            "username":   f"api_key:{matched['name']}",
            "role":       matched["role"],
            "is_active":  True,
            "_auth_type": "api_key",
            "_key_id":    matched["id"],
            "_key_name":  matched["name"],
        }

    # ── Path 2: JWT Bearer Token ──────────────────────────────────────────────
    if not token:
        raise credentials_exc

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
    if role not in _ADMIN_ROLES:
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
    if role not in _OFFICE_ROLES:
        logger.warning(
            f"Dispatch/Admin access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Manifest access required",
        )
    return current_user


def require_office(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Raises 403 unless the caller has ADMIN or DISPATCH role.

    Covers office-side delivery oversight operations (e.g. viewing full audit
    trails) that field drivers must not access.
    """
    role = current_user.get("role", "")
    if role not in _OFFICE_ROLES:
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
    if role not in _OFFICE_READ_ROLES:
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
    if role not in _DELIVERY_WRITE_ROLES:
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
    if role not in _DELIVERY_READ_ROLES:
        logger.warning(
            f"Delivery read access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Delivery access required",
        )
    return current_user


def require_driver(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Raises 403 unless the caller has DRIVER role.

    Used for endpoints that are exclusively field actions performed by a driver
    in a truck — e.g. submitting GPS location pings.  Office roles (ADMIN,
    DISPATCH, REPORTS_ONLY) are explicitly blocked; they must use a DRIVER
    account for any testing that requires this guard.
    """
    role = current_user.get("role", "")
    if role not in _DRIVER_ROLES:
        logger.warning(
            f"Driver-only access denied for user '{current_user.get('username')}' (role={role})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Driver access required",
        )
    return current_user

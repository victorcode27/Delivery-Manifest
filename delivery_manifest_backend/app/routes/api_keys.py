"""
app/routes/api_keys.py

Admin-only endpoints for managing static API keys.

These keys are intended for AI agents, automation scripts, and external
integrations that cannot handle JWT token refresh.

Endpoints
---------
GET    /api/api-keys           — list all keys (no hashes, no raw keys)
POST   /api/api-keys           — create a new key (raw key returned ONCE)
DELETE /api/api-keys/{key_id}  — revoke (deactivate) a key
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from delivery_manifest_backend.app.core.deps import get_current_user, require_admin
from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.core.security import generate_api_key, VALID_ROLES
from delivery_manifest_backend.app.db.database import get_db_session
from delivery_manifest_backend.app.schemas.api_key import (
    ApiKeyCreate,
    ApiKeyListItem,
    ApiKeyResponse,
    ApiKeyRevokeResponse,
)

router = APIRouter(prefix="/api-keys", tags=["api-keys"])
logger = get_logger(__name__)


@router.get("", response_model=list[ApiKeyListItem])
def list_api_keys(current_user: dict = Depends(require_admin)):
    """
    Return all API keys (active and revoked).

    The raw key and bcrypt hash are never returned.
    Requires ADMIN role.
    """
    db = get_db_session()
    try:
        rows = db.execute(text("""
            SELECT id, name, key_prefix, role, is_active,
                   created_by, created_at, last_used_at, expires_at
            FROM api_keys
            ORDER BY created_at DESC
        """)).fetchall()
        return [dict(r._mapping) for r in rows]
    finally:
        db.close()


@router.post("", response_model=ApiKeyResponse, status_code=201)
def create_api_key(
    body: ApiKeyCreate,
    current_user: dict = Depends(require_admin),
):
    """
    Create a new API key.

    The ``raw_key`` field in the response is shown **only once** — store it
    immediately. It cannot be retrieved again (only its bcrypt hash is stored).

    Requires ADMIN role.
    """
    if body.role not in VALID_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role '{body.role}'. Must be one of: {', '.join(VALID_ROLES)}",
        )

    raw_key, prefix, key_hash = generate_api_key()

    db = get_db_session()
    try:
        result = db.execute(
            text("""
                INSERT INTO api_keys
                    (name, key_prefix, key_hash, role, is_active, created_by, expires_at)
                VALUES
                    (:name, :prefix, :key_hash, :role, TRUE, :created_by, :expires_at)
                RETURNING id, name, key_prefix, role, is_active, created_at, expires_at
            """),
            {
                "name":       body.name,
                "prefix":     prefix,
                "key_hash":   key_hash,
                "role":       body.role,
                "created_by": current_user.get("id"),
                "expires_at": body.expires_at,
            },
        )
        row = dict(result.fetchone()._mapping)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error(f"Failed to create API key: {exc}")
        raise HTTPException(status_code=500, detail="Failed to create API key")
    finally:
        db.close()

    logger.info(
        f"API key created: '{body.name}' (role={body.role}) "
        f"by user '{current_user.get('username')}'"
    )

    return {
        **row,
        "raw_key": raw_key,   # returned ONCE — never stored, never returned again
    }


@router.delete("/{key_id}", response_model=ApiKeyRevokeResponse)
def revoke_api_key(
    key_id: int,
    current_user: dict = Depends(require_admin),
):
    """
    Revoke an API key by setting ``is_active = FALSE``.

    The key record is preserved for audit purposes.
    Requires ADMIN role.
    """
    db = get_db_session()
    try:
        result = db.execute(
            text("""
                UPDATE api_keys
                SET is_active = FALSE
                WHERE id = :key_id
                RETURNING id, name
            """),
            {"key_id": key_id},
        )
        row = result.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"API key {key_id} not found")
        db.commit()
        row_dict = dict(row._mapping)
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.error(f"Failed to revoke API key {key_id}: {exc}")
        raise HTTPException(status_code=500, detail="Failed to revoke API key")
    finally:
        db.close()

    logger.info(
        f"API key '{row_dict['name']}' (id={key_id}) revoked "
        f"by user '{current_user.get('username')}'"
    )
    return {"message": f"API key '{row_dict['name']}' has been revoked"}

"""
app/routes/users.py

User management endpoints — FULL_ACCESS only.

GET    /api/users                → list all users
POST   /api/users                → create user
PUT    /api/users/{id}/password  → reset password
PUT    /api/users/{id}/role      → update access level
PUT    /api/users/{id}/status    → activate / deactivate

All routes require a valid JWT with FULL_ACCESS role.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from delivery_manifest_backend.app.core.deps import require_full_access
from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.db.database import get_db
from delivery_manifest_backend.app.schemas.user import (
    PasswordReset,
    RoleUpdate,
    StatusUpdate,
    UserCreate,
)
from delivery_manifest_backend.app.services import user_service

router = APIRouter(prefix="/users", tags=["users"])
logger = get_logger(__name__)


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("")
def get_users(
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_full_access),
):
    """Return all users (without password hashes)."""
    try:
        users = user_service.get_all_users(db)
        return {"users": users}
    except Exception:
        logger.error("Error fetching users", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("")
def create_user(
    request: UserCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_full_access),
):
    """Create a new user account."""
    try:
        user = user_service.create_user(
            db, request.username, request.password, request.role, request.is_active
        )
        logger.info(f"Created user '{request.username}' by '{current_user.get('username')}'")
        return {"message": f"User '{request.username}' created", "user": user.to_dict()}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.error("Error creating user", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ── Password Reset ────────────────────────────────────────────────────────────

@router.put("/{user_id}/password")
def reset_password(
    user_id: int,
    request: PasswordReset,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_full_access),
):
    """Reset a user's password."""
    try:
        user = user_service.reset_password(db, user_id, request.password)
        logger.info(f"Password reset for '{user.username}' by '{current_user.get('username')}'")
        return {"message": f"Password reset for '{user.username}'"}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.error("Error resetting password", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ── Role Update ───────────────────────────────────────────────────────────────

@router.put("/{user_id}/role")
def update_role(
    user_id: int,
    request: RoleUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_full_access),
):
    """Change a user's access level."""
    try:
        user = user_service.update_role(db, user_id, request.role, current_user)
        logger.info(f"Role updated for '{user.username}' → '{request.role}' by '{current_user.get('username')}'")
        return {"message": f"Role updated for '{user.username}'", "user": user.to_dict()}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.error("Error updating role", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ── Status Update ─────────────────────────────────────────────────────────────

@router.put("/{user_id}/status")
def update_status(
    user_id: int,
    request: StatusUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_full_access),
):
    """Activate or deactivate a user account."""
    try:
        user = user_service.update_status(db, user_id, request.is_active, current_user)
        action = "activated" if request.is_active else "deactivated"
        logger.info(f"User '{user.username}' {action} by '{current_user.get('username')}'")
        return {"message": f"User '{user.username}' {action}", "user": user.to_dict()}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.error("Error updating status", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

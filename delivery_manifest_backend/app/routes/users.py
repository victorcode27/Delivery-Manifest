"""
app/routes/users.py

User management endpoints — admin only.

GET    /users            → list all users
POST   /users            → create user
PUT    /users/{username} → update user
DELETE /users/{username} → delete user

All routes require a valid admin JWT (require_admin dependency).
"""

from fastapi import APIRouter, Depends, HTTPException

from delivery_manifest_backend.app.core.deps import require_admin
from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.schemas.user import UserCreate, UserUpdate
from delivery_manifest_backend.app.services import user_service

router = APIRouter(prefix="/users", tags=["users"])
logger = get_logger(__name__)


@router.get("")
def get_users(current_user: dict = Depends(require_admin)):
    """Return all users (without password hashes)."""
    try:
        return {"users": user_service.get_all_users()}
    except Exception:
        logger.error("Error fetching users", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("")
def create_user(
    request: UserCreate,
    current_user: dict = Depends(require_admin),
):
    """Create a new user account."""
    try:
        ok = user_service.create_user(
            request.username, request.password, request.is_admin, request.can_manifest
        )
        if not ok:
            raise HTTPException(status_code=400, detail="Username already exists")
        logger.info(f"Created user '{request.username}'")
        return {"message": f"User '{request.username}' created"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error creating user", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{username}")
def update_user(
    username: str,
    request: UserUpdate,
    current_user: dict = Depends(require_admin),
):
    """Update password and/or permissions for an existing user."""
    try:
        ok = user_service.update_user(
            username, request.password, request.is_admin, request.can_manifest
        )
        if not ok:
            raise HTTPException(status_code=404, detail="User not found")
        logger.info(f"Updated user '{username}'")
        return {"message": f"User '{username}' updated"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error updating user", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/{username}")
def delete_user(
    username: str,
    current_user: dict = Depends(require_admin),
):
    """Delete a user account."""
    try:
        ok = user_service.delete_user(username)
        if not ok:
            raise HTTPException(status_code=404, detail="User not found")
        logger.info(f"Deleted user '{username}'")
        return {"message": f"User '{username}' deleted"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error deleting user", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

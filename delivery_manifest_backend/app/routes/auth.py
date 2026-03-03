"""
app/routes/auth.py

Authentication endpoints.

POST /api/auth/login  → verify credentials, return JWT access token
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.core.security import create_access_token
from delivery_manifest_backend.app.db.database import get_db
from delivery_manifest_backend.app.schemas.user import LoginRequest
from delivery_manifest_backend.app.services.user_service import verify_user

router = APIRouter(prefix="/auth", tags=["auth"])
logger = get_logger(__name__)


@router.post("/login")
def login(request: LoginRequest, db: Session = Depends(get_db)):
    """
    Verify credentials and return a signed JWT access token.

    Returns 401 on bad credentials and 403 if the account is inactive.
    """
    user = verify_user(db, request.username, request.password)
    if not user:
        logger.warning(f"Failed login attempt for '{request.username}'")
        raise HTTPException(status_code=401, detail="Invalid username or password")

    if not user.is_active:
        logger.warning(f"Inactive account login attempt: '{request.username}'")
        raise HTTPException(status_code=403, detail="Account is inactive")

    token = create_access_token({
        "sub":  user.username,
        "role": user.role,
    })

    logger.info(f"User '{request.username}' logged in (role={user.role})")
    return {
        "success":      True,
        "access_token": token,
        "token_type":   "bearer",
        "user": {
            "id":       user.id,
            "username": user.username,
            "role":     user.role,
            "isActive": user.is_active,
        },
    }

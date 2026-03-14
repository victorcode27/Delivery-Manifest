"""
app/services/user_service.py

Business logic for user management.

All functions receive a SQLAlchemy ``Session`` from the route layer (via
``Depends(get_db)``).  No manual session open/close here.

Security rules enforced:
  • Password strength validated before hashing
  • Self-lockout prevention (cannot change own role / status)
  • Role values constrained to ADMIN | DISPATCH | REPORTS_ONLY
"""

from typing import Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.core.security import (
    hash_password,
    validate_password_strength,
    verify_password,
)
from delivery_manifest_backend.app.models.user import User

logger = get_logger(__name__)

VALID_ROLES = ("ADMIN", "DISPATCH", "REPORTS_ONLY", "DRIVER")


# ── Read ──────────────────────────────────────────────────────────────────────

def get_all_users(db: Session) -> List[Dict]:
    """Return all users without their password hashes."""
    users = db.query(User).order_by(User.id).all()
    return [u.to_dict() for u in users]


def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
    """Return a User ORM instance or None."""
    return db.query(User).filter(User.id == user_id).first()


def get_user_by_username(db: Session, username: str) -> Optional[User]:
    """Return a User ORM instance or None."""
    return db.query(User).filter(User.username == username).first()


# ── Auth ──────────────────────────────────────────────────────────────────────

def verify_user(db: Session, username: str, password: str) -> Optional[User]:
    """
    Verify credentials.  Returns the User on success, None on failure.
    """
    user = get_user_by_username(db, username)
    if user and verify_password(password, user.hashed_password):
        return user
    return None


# ── Create ────────────────────────────────────────────────────────────────────

def create_user(
    db: Session,
    username: str,
    password: str,
    role: str = "ADMIN",
    is_active: bool = True,
) -> User:
    """
    Insert a new user.

    Raises:
        ValueError  – username taken or invalid role or weak password
    """
    # Password policy (also validated by schema, but belt-and-braces)
    pw_errors = validate_password_strength(password)
    if pw_errors:
        raise ValueError("; ".join(pw_errors))

    if role not in VALID_ROLES:
        raise ValueError(f"Invalid role: {role}")

    user = User(
        username=username,
        hashed_password=hash_password(password),
        role=role,
        is_active=is_active,
    )
    db.add(user)
    try:
        db.commit()
        db.refresh(user)
    except IntegrityError:
        db.rollback()
        raise ValueError(f"Username '{username}' already exists")
    logger.info(f"Created user '{username}' with role '{role}'")
    return user


# ── Password Reset ────────────────────────────────────────────────────────────

def reset_password(db: Session, user_id: int, new_password: str) -> User:
    """
    Reset a user's password.

    Raises:
        ValueError – user not found or weak password
    """
    pw_errors = validate_password_strength(new_password)
    if pw_errors:
        raise ValueError("; ".join(pw_errors))

    user = get_user_by_id(db, user_id)
    if not user:
        raise ValueError("User not found")

    user.hashed_password = hash_password(new_password)
    db.commit()
    db.refresh(user)
    logger.info(f"Password reset for user '{user.username}'")
    return user


# ── Role Update ───────────────────────────────────────────────────────────────

def update_role(
    db: Session,
    target_id: int,
    new_role: str,
    current_user: Optional[dict] = None,
) -> User:
    """
    Change a user's access level.

    Raises:
        ValueError – user not found, invalid role, or self-lockout attempt
    """
    if current_user and current_user.get("id") == target_id:
        raise ValueError("Cannot change your own role")

    if new_role not in VALID_ROLES:
        raise ValueError(f"Invalid role: {new_role}")

    user = get_user_by_id(db, target_id)
    if not user:
        raise ValueError("User not found")

    user.role = new_role
    db.commit()
    db.refresh(user)
    logger.info(f"Role updated for '{user.username}' → '{new_role}'")
    return user


# ── Status Update ─────────────────────────────────────────────────────────────

def update_status(
    db: Session,
    target_id: int,
    is_active: bool,
    current_user: Optional[dict] = None,
) -> User:
    """
    Activate or deactivate a user account.

    Raises:
        ValueError – user not found or self-lockout attempt
    """
    if current_user and current_user.get("id") == target_id:
        raise ValueError("Cannot change your own status")

    user = get_user_by_id(db, target_id)
    if not user:
        raise ValueError("User not found")

    user.is_active = is_active
    db.commit()
    db.refresh(user)
    logger.info(f"Status updated for '{user.username}' → active={is_active}")
    return user

"""
app/services/user_service.py

Business logic for user management.
All direct database interaction (session open/close, SQL) lives here.
Routes call these functions and only deal with HTTP concerns.
"""

from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.exc import IntegrityError

from app.core.logger import get_logger
from app.core.security import hash_password, verify_password
from app.db.database import get_db_session, execute_query

logger = get_logger(__name__)


# ── Read ──────────────────────────────────────────────────────────────────────

def get_user(username: str) -> Optional[Dict]:
    """Return a user dict (including password_hash) or None."""
    db = get_db_session()
    try:
        result = execute_query(db, "SELECT * FROM users WHERE username = ?", (username,))
        row = result.fetchone()
        return dict(row._mapping) if row else None
    finally:
        db.close()


def get_all_users() -> List[Dict]:
    """Return all users without their password hashes."""
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "SELECT id, username, is_admin, can_manifest, created_at FROM users",
        )
        return [dict(row._mapping) for row in result.fetchall()]
    finally:
        db.close()


# ── Auth ──────────────────────────────────────────────────────────────────────

def verify_user(username: str, password: str) -> Optional[Dict]:
    """
    Verify credentials.  Returns the user dict on success, None on failure.
    """
    user = get_user(username)
    if user and verify_password(password, user["password_hash"]):
        return user
    return None


# ── Create / Update / Delete ──────────────────────────────────────────────────

def create_user(
    username: str,
    password: str,
    is_admin: bool = False,
    can_manifest: bool = True,
) -> bool:
    """
    Insert a new user.  Returns True on success, False if username is taken.
    """
    db = get_db_session()
    try:
        execute_query(
            db,
            """
            INSERT INTO users (username, password_hash, is_admin, can_manifest, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                username,
                hash_password(password),
                1 if is_admin else 0,
                1 if can_manifest else 0,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        db.commit()
        return True
    except IntegrityError:
        logger.warning(f"Duplicate username attempted: '{username}'")
        return False
    finally:
        db.close()


def update_user(
    username: str,
    password: Optional[str] = None,
    is_admin: Optional[bool] = None,
    can_manifest: Optional[bool] = None,
) -> bool:
    """
    Update one or more user fields.  Returns True if a row was modified.
    """
    updates: list[str] = []
    params:  list      = []

    if password is not None:
        updates.append("password_hash = ?")
        params.append(hash_password(password))
    if is_admin is not None:
        updates.append("is_admin = ?")
        params.append(1 if is_admin else 0)
    if can_manifest is not None:
        updates.append("can_manifest = ?")
        params.append(1 if can_manifest else 0)

    if not updates:
        return False

    params.append(username)
    db = get_db_session()
    try:
        result = execute_query(
            db,
            f"UPDATE users SET {', '.join(updates)} WHERE username = ?",
            params,
        )
        updated = result.rowcount > 0
        db.commit()
        return updated
    finally:
        db.close()


def delete_user(username: str) -> bool:
    """Delete a user by username.  Returns True if a row was removed."""
    db = get_db_session()
    try:
        result = execute_query(db, "DELETE FROM users WHERE username = ?", (username,))
        deleted = result.rowcount > 0
        db.commit()
        return deleted
    finally:
        db.close()

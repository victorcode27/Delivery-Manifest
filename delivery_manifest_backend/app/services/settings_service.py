"""
app/services/settings_service.py

Reference-data CRUD for settings, trucks, and customer-route mappings.
Extracted from manifest_service.py — zero coupling to the core manifest pipeline.
"""

from typing import Dict, List, Optional

from sqlalchemy.exc import IntegrityError

from delivery_manifest_backend.app.db.database import execute_query, get_db_session
from delivery_manifest_backend.app.core.logger import get_logger

logger = get_logger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# SETTINGS
# ══════════════════════════════════════════════════════════════════════════════

def get_settings(category: str) -> List[str]:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "SELECT value FROM settings WHERE category = ? ORDER BY value",
            (category,),
        )
        return [row._mapping["value"] for row in result.fetchall()]
    finally:
        db.close()


def add_setting(category: str, value: str) -> bool:
    db = get_db_session()
    try:
        execute_query(
            db, "INSERT INTO settings (category, value) VALUES (?, ?)", (category, value)
        )
        db.commit()
        return True
    except IntegrityError:
        return False
    finally:
        db.close()


def update_setting(category: str, old_value: str, new_value: str) -> bool:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "UPDATE settings SET value = ? WHERE category = ? AND value = ?",
            (new_value, category, old_value),
        )
        db.commit()
        return result.rowcount > 0
    except IntegrityError:
        return False
    finally:
        db.close()


def delete_setting(category: str, value: str) -> bool:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "DELETE FROM settings WHERE category = ? AND value = ?",
            (category, value),
        )
        db.commit()
        return result.rowcount > 0
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# TRUCKS
# ══════════════════════════════════════════════════════════════════════════════

def get_trucks() -> List[Dict]:
    db = get_db_session()
    try:
        result = execute_query(db, "SELECT * FROM trucks ORDER BY reg")
        return [dict(row._mapping) for row in result.fetchall()]
    finally:
        db.close()


def add_truck(
    reg: str,
    driver: Optional[str] = None,
    assistant: Optional[str] = None,
    checker: Optional[str] = None,
) -> bool:
    db = get_db_session()
    try:
        execute_query(
            db,
            "INSERT INTO trucks (reg, driver, assistant, checker) VALUES (?, ?, ?, ?)",
            (reg, driver, assistant, checker),
        )
        db.commit()
        return True
    except IntegrityError:
        return False
    finally:
        db.close()


def update_truck(
    reg: str,
    driver: Optional[str] = None,
    assistant: Optional[str] = None,
    checker: Optional[str] = None,
) -> bool:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "UPDATE trucks SET driver = ?, assistant = ?, checker = ? WHERE reg = ?",
            (driver, assistant, checker, reg),
        )
        db.commit()
        return result.rowcount > 0
    finally:
        db.close()


def delete_truck(reg: str) -> bool:
    db = get_db_session()
    try:
        result = execute_query(db, "DELETE FROM trucks WHERE reg = ?", (reg,))
        db.commit()
        return result.rowcount > 0
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# CUSTOMER ROUTES
# ══════════════════════════════════════════════════════════════════════════════

def get_customer_routes() -> List[Dict]:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "SELECT customer_name, route_name, delivery_mode "
            "FROM customer_routes ORDER BY route_name, customer_name",
        )
        return [dict(r._mapping) for r in result.fetchall()]
    finally:
        db.close()


def add_customer_route(customer_name: str, route_name: str, delivery_mode: str = "INTERNAL") -> bool:
    db = get_db_session()
    try:
        execute_query(
            db,
            "REPLACE INTO customer_routes (customer_name, route_name, delivery_mode) VALUES (?, ?, ?)",
            (customer_name, route_name, delivery_mode),
        )
        db.commit()
        return True
    except IntegrityError:
        return False
    finally:
        db.close()


def delete_customer_route(customer_name: str) -> bool:
    db = get_db_session()
    try:
        result = execute_query(
            db,
            "DELETE FROM customer_routes WHERE customer_name = ?",
            (customer_name,),
        )
        db.commit()
        return result.rowcount > 0
    finally:
        db.close()

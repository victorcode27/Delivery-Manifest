"""
Database module for the Delivery Manifest System.
Uses PostgreSQL via SQLAlchemy (SessionLocal from db_config).
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Optional
import hashlib
import bcrypt
from db_config import SessionLocal
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError



def get_session():
    """FastAPI dependency injection session (generator). Use with Depends()."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session():
    """
    Manual session for use outside FastAPI (scripts, background tasks).

    Usage:
        db = get_db_session()
        try:
            ...
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
    """
    return SessionLocal()


def execute_sqlite_wrapper(db, query, params=None):
    """
    Helper to convert SQLite `?` to SQLAlchemy `:p0` named parameters.
    """
    if "REPLACE INTO" in query.upper():
        query = query.replace("REPLACE INTO", "INSERT INTO")
        if "customer_routes" in query:
             query += " ON CONFLICT (customer_name) DO UPDATE SET route_name = EXCLUDED.route_name"

    if params:
        if '?' in query:
            parts = query.split('?')
            new_query = parts[0]
            named_params = {}
            for idx, val in enumerate(params):
                param_name = f"p{idx}"
                new_query += f":{param_name}" + parts[idx+1]
                named_params[param_name] = val
            query = new_query
            params = named_params
        elif isinstance(params, (tuple, list)):
             pass
    else:
        params = {}
        
    query_upper = query.strip().upper()
    if query_upper.startswith("INSERT ") and "RETURNING" not in query_upper:
        query = query.rstrip(";\t\n\r ") + " RETURNING id"
        
    result = db.execute(text(query), params)
    return result


def init_db():
    """Initialize the database with required tables."""
    db = SessionLocal()
    
    # Orders table - stores invoice/order data
    # Added type, reference_number, original_value, status for Credit Note support
    result = execute_sqlite_wrapper(db, '''
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            filename TEXT UNIQUE NOT NULL,
            date_processed TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            total_value TEXT DEFAULT '0.00',
            order_number TEXT DEFAULT 'N/A',
            invoice_number TEXT DEFAULT 'N/A',
            invoice_date TEXT DEFAULT 'N/A',
            area TEXT DEFAULT 'UNKNOWN',
            is_allocated INTEGER DEFAULT 0,
            allocated_date TEXT,
            manifest_number TEXT,
            manifest_number TEXT,
            type TEXT DEFAULT 'INVOICE',
            reference_number TEXT,
            original_value TEXT,
            status TEXT DEFAULT 'PENDING',
            customer_number TEXT DEFAULT 'N/A'  -- Added Customer Number
        )
    ''')
    
    # Check if we need to migrate existing table (add new columns if missing)
    try:
        result = execute_sqlite_wrapper(db, "SELECT customer_number FROM orders LIMIT 1")
    except OperationalError:
        print("Migrating database: Adding customer_number column...")
        try:
            result = execute_sqlite_wrapper(db, "ALTER TABLE orders ADD COLUMN customer_number TEXT DEFAULT 'N/A'")
        except Exception as e:
            print(f"Migration warning: {e}")

    # Users table... (rest remains same)
    
    # Reports table... (rest remains same)

    # Report items table - links invoices to reports
    result = execute_sqlite_wrapper(db, '''
        CREATE TABLE IF NOT EXISTS report_items (
            id SERIAL PRIMARY KEY,
            report_id INTEGER NOT NULL,
            invoice_number TEXT NOT NULL,
            order_number TEXT,
            customer_name TEXT,
            customer_number TEXT,  -- Added Customer Number
            invoice_date TEXT,
            area TEXT,
            sku INTEGER DEFAULT 0,
            value REAL DEFAULT 0,
            weight REAL DEFAULT 0,
            FOREIGN KEY (report_id) REFERENCES reports(id)
        )
    ''')
    
    # Add migration for report_items too
    try:
        result = execute_sqlite_wrapper(db, "SELECT customer_number FROM report_items LIMIT 1")
    except OperationalError:
        print("Migrating database: Adding customer_number to report_items...")
        try:
            result = execute_sqlite_wrapper(db, "ALTER TABLE report_items ADD COLUMN customer_number TEXT DEFAULT 'N/A'")
        except Exception as e:
            print(f"Migration warning (report_items): {e}")
    
    # Settings table - stores app settings (routes, drivers, etc.)
    result = execute_sqlite_wrapper(db, '''
        CREATE TABLE IF NOT EXISTS settings (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            value TEXT NOT NULL,
            UNIQUE(category, value)
        )
    ''')
    
    # Trucks table
    result = execute_sqlite_wrapper(db, '''
        CREATE TABLE IF NOT EXISTS trucks (
            id SERIAL PRIMARY KEY,
            reg TEXT UNIQUE NOT NULL,
            driver TEXT,
            assistant TEXT,
            checker TEXT
        )
    ''')

    # Customer Routes table
    result = execute_sqlite_wrapper(db, '''
        CREATE TABLE IF NOT EXISTS customer_routes (
            id SERIAL PRIMARY KEY,
            customer_name TEXT UNIQUE NOT NULL,
            route_name TEXT NOT NULL
        )
    ''')

    # Manifest Events table (Audit Trail)
    result = execute_sqlite_wrapper(db, '''
        CREATE TABLE IF NOT EXISTS manifest_events (
            id SERIAL PRIMARY KEY,
            manifest_number TEXT NOT NULL,
            event_type TEXT NOT NULL,
            performed_by TEXT DEFAULT 'System',
            timestamp TEXT NOT NULL
        )
    ''')
    
    # Manifest Staging table (FIX: Prevents invoices from disappearing)
    result = execute_sqlite_wrapper(db, '''
        CREATE TABLE IF NOT EXISTS manifest_staging (
            id SERIAL PRIMARY KEY,
            session_id TEXT NOT NULL,
            invoice_id INTEGER NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES orders(id)
        )
    ''')
    
    # Create index for faster staging queries
    result = execute_sqlite_wrapper(db, '''
        CREATE INDEX IF NOT EXISTS idx_staging_session 
        ON manifest_staging(session_id)
    ''')
    
    db.commit()
    db.close()
    print("Database schema verified via SQLAlchemy")
    
    # Check if we need to create a default admin user
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'SELECT count(*) FROM users')
    if result.fetchone()[0] == 0:
        pass_hash = hashlib.sha256("admin".encode()).hexdigest()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = execute_sqlite_wrapper(db, '''
            INSERT INTO users (username, password_hash, is_admin, can_manifest, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', ('admin', pass_hash, 1, 1, now))
        db.commit()
        print("Created default admin user (admin/admin)")
    db.close()

# =============================================
# ORDER FUNCTIONS
# =============================================

def get_all_orders(allocated: bool = False) -> List[Dict]:
    """Get all orders. If allocated=False, only return pending orders."""
    db = SessionLocal()
    
    # Modified to only show INVOICE type in the main lists and filter by status
    if allocated:
        # Show allocated invoices
        result = execute_sqlite_wrapper(db, "SELECT * FROM orders WHERE type = 'INVOICE' ORDER BY date_processed DESC")
    else:
        # Show pending invoices (not allocated and not cancelled)
        result = execute_sqlite_wrapper(db, "SELECT * FROM orders WHERE is_allocated = 0 AND type = 'INVOICE' AND status != 'CANCELLED' ORDER BY date_processed DESC")
    
    rows = result.fetchall()
    db.close()
    
    # Convert to list of dicts matching the old JSON format
    return [dict(row._mapping) for row in rows]

def get_order_by_filename(filename: str) -> Optional[Dict]:
    """Get a single order by filename."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'SELECT * FROM orders WHERE filename = ?', (filename,))
    row = result.fetchone()
    db.close()
    return dict(row._mapping) if row else None
    
def get_order_by_invoice_number(invoice_number: str) -> Optional[Dict]:
    """Get a single order by invoice number."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, "SELECT * FROM orders WHERE invoice_number = ? AND type = 'INVOICE'", (invoice_number,))
    row = result.fetchone()
    db.close()
    return dict(row._mapping) if row else None

def update_order_value(invoice_number: str, new_value: str, original_value: str = None) -> bool:
    """Update the value of an order (used for Partial Credit)."""
    db = SessionLocal()
    try:
        if original_value:
             result = execute_sqlite_wrapper(db, "UPDATE orders SET total_value = ?, original_value = ? WHERE invoice_number = ?", 
                           (new_value, original_value, invoice_number))
        else:
             result = execute_sqlite_wrapper(db, "UPDATE orders SET total_value = ? WHERE invoice_number = ?", 
                           (new_value, invoice_number))
        updated = result.rowcount > 0
        db.commit()
        db.close()
        return updated
    except Exception as e:
        print(f"Error updating order value: {e}")
        db.close()
        return False

def cancel_order(invoice_number: str) -> bool:
    """Mark an order as CANCELLED (used for Full Credit)."""
    db = SessionLocal()
    try:
        result = execute_sqlite_wrapper(db, "UPDATE orders SET status = 'CANCELLED' WHERE invoice_number = ?", (invoice_number,))
        updated = result.rowcount > 0
        db.commit()
        db.close()
        return updated
    except Exception as e:
        print(f"Error cancelling order: {e}")
        db.close()
        return False

def add_order(order_data: Dict) -> bool:
    """Add a new order/credit note to the database. Returns True if successful."""
    db = SessionLocal()
    
    try:
        result = execute_sqlite_wrapper(db, '''
            INSERT INTO orders (filename, date_processed, customer_name, total_value, 
                              order_number, invoice_number, invoice_date, area,
                              type, reference_number, original_value, status, customer_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            order_data.get('filename'),
            order_data.get('date_processed', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            order_data.get('customer_name', 'Unknown'),
            order_data.get('total_value', '0.00'),
            order_data.get('order_number', 'N/A'),
            order_data.get('invoice_number', 'N/A'),
            order_data.get('invoice_date', 'N/A'),
            order_data.get('area', 'UNKNOWN'),
            order_data.get('type', 'INVOICE'),
            order_data.get('reference_number', None),
            order_data.get('original_value', None),
            order_data.get('status', 'PENDING'),
            order_data.get('customer_number', 'N/A')
        ))
        db.commit()
        db.close()
        return True
    except IntegrityError:
        # Duplicate filename
        db.close()
        return False

def allocate_orders(filenames: List[str], manifest_number: str = None) -> int:
    """Mark orders as allocated. Returns count of updated orders."""
    db = SessionLocal()
    
    allocated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    placeholders = ','.join(['?' for _ in filenames])
    
    result = execute_sqlite_wrapper(db, f'''
        UPDATE orders 
        SET is_allocated = 1, allocated_date = ?, manifest_number = ?
        WHERE filename IN ({placeholders})
    ''', [allocated_date, manifest_number] + filenames)
    
    updated = result.rowcount
    db.commit()
    db.close()
    return updated

def get_areas() -> List[str]:
    """Get unique areas from all orders."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'SELECT DISTINCT area FROM orders WHERE area != "UNKNOWN" ORDER BY area')
    rows = result.fetchall()
    db.close()
    return [row._mapping['area'] for row in rows]

def get_all_customers() -> List[str]:
    """Get unique customer names from all orders (pending and allocated)."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'SELECT DISTINCT customer_name FROM orders ORDER BY customer_name')
    rows = result.fetchall()
    db.close()
    return [row._mapping['customer_name'] for row in rows]

def search_orders(query: str) -> List[Dict]:
    """Search for orders (pending or allocated) by invoice, order #, or customer."""
    db = SessionLocal()
    
    # We want to match partial strings
    like_query = f"%{query}%"
    
    result = execute_sqlite_wrapper(db, '''
        SELECT * FROM orders 
        WHERE invoice_number LIKE ? 
            OR order_number LIKE ? 
            OR customer_name LIKE ?
            OR filename LIKE ?
            OR customer_number LIKE ?
        ORDER BY date_processed DESC
        LIMIT 50
    ''', (like_query, like_query, like_query, like_query, like_query))
    
    rows = result.fetchall()
    db.close()
    return [dict(row._mapping) for row in rows]

def deallocate_orders(filenames: List[str]) -> int:
    """Reset orders to pending status (un-allocate). Returns count of updated orders."""
    db = SessionLocal()
    
    if not filenames:
        return 0

    placeholders = ','.join(['?' for _ in filenames])
    
    result = execute_sqlite_wrapper(db, f'''
        UPDATE orders 
        SET is_allocated = 0, allocated_date = NULL, manifest_number = NULL
        WHERE filename IN ({placeholders})
    ''', filenames)
    
    updated = result.rowcount
    db.commit()
    db.close()
    return updated

def get_available_orders_excluding_staging():
    """Return orders not allocated and not present in manifest staging."""
    db = SessionLocal()

    result = execute_sqlite_wrapper(db, """
        SELECT *
        FROM orders o
        WHERE NOT EXISTS (
            SELECT 1 FROM manifest_staging ms
            WHERE ms.invoice_id = o.id
        )
        AND o.type = 'INVOICE'
        AND o.status != 'CANCELLED'
        AND (
            o.manifest_number IS NULL
            OR NOT EXISTS (
                SELECT 1 FROM reports r
                WHERE r.manifest_number = o.manifest_number
            )
        )
        ORDER BY o.date_processed DESC
    """)

    rows = result.fetchall()
    db.close()
    return [dict(row._mapping) for row in rows]

# =============================================
# MANIFEST STAGING FUNCTIONS (FIX FOR WORKFLOW BUG)
# =============================================

def add_to_staging(session_id: str, filenames: List[str]) -> int:
    """Add invoices to manifest staging. Invoices remain AVAILABLE until confirmed. Returns count added."""
    db = SessionLocal()
    
    if not filenames or not session_id:
        return 0
    
    # Get invoice IDs from filenames
    placeholders = ','.join(['?' for _ in filenames])
    result = execute_sqlite_wrapper(db, f'''
        SELECT id, filename FROM orders 
        WHERE filename IN ({placeholders})
    ''', filenames)
    
    invoice_rows = result.fetchall()
    added_count = 0
    
    for row in invoice_rows:
        invoice_id = row._mapping['id']
        # Check if already in staging for this session
        result = execute_sqlite_wrapper(db, '''
            SELECT id FROM manifest_staging 
            WHERE session_id = ? AND invoice_id = ?
        ''', (session_id, invoice_id))
        
        if result.fetchone() is None:
            # Not in staging, add it
            result = execute_sqlite_wrapper(db, '''
                INSERT INTO manifest_staging (session_id, invoice_id)
                VALUES (?, ?)
            ''', (session_id, invoice_id))
            added_count += 1
    
    db.commit()
    db.close()
    return added_count

def get_current_manifest(session_id: str, manifest_number: str = None) -> List[Dict]:
    """Get all invoices for the current manifest.
    
    Returns:
    - Finalized invoices (orders.manifest_number = manifest_number AND is_allocated=1)
    - Plus in-progress staging entries for this session (that aren't already finalized)
    - Only type='INVOICE' rows, no duplicates
    """
    db = SessionLocal()
    
    if manifest_number:
        # Return finalized invoices for this manifest UNION with NEW staging entries only
        result = execute_sqlite_wrapper(db, '''
            SELECT o.* FROM orders o
            WHERE o.manifest_number = ? AND o.is_allocated = 1 AND o.type = 'INVOICE'
            UNION
            SELECT o.* FROM orders o
            INNER JOIN manifest_staging ms ON ms.invoice_id = o.id
            WHERE ms.session_id = ? 
            AND o.type = 'INVOICE'
            AND o.is_allocated = 0
            AND (o.manifest_number IS NULL OR o.manifest_number != ?)
            ORDER BY date_processed DESC
        ''', (manifest_number, session_id, manifest_number))
    else:
        # Staging only (backward compatible) - exclude already allocated
        result = execute_sqlite_wrapper(db, '''
            SELECT o.* 
            FROM orders o
            INNER JOIN manifest_staging ms ON ms.invoice_id = o.id
            WHERE ms.session_id = ? 
            AND o.type = 'INVOICE'
            AND o.is_allocated = 0
            ORDER BY ms.added_at ASC
        ''', (session_id,))
    
    rows = result.fetchall()
    db.close()
    return [dict(row._mapping) for row in rows]

def remove_from_staging(session_id: str, filenames: List[str]) -> int:
    """Remove invoices from manifest staging. Makes them instantly available again. Returns count removed."""
    db = SessionLocal()
    
    if not filenames or not session_id:
        return 0
    
    # Get invoice IDs from filenames
    placeholders = ','.join(['?' for _ in filenames])
    result = execute_sqlite_wrapper(db, f'''
        SELECT id FROM orders 
        WHERE filename IN ({placeholders})
    ''', filenames)
    
    invoice_ids = [row['id'] for row in result.fetchall()]
    
    if not invoice_ids:
        db.close()
        return 0
    
    # Delete from staging
    id_placeholders = ','.join(['?' for _ in invoice_ids])
    result = execute_sqlite_wrapper(db, f'''
        DELETE FROM manifest_staging
        WHERE session_id = ? AND invoice_id IN ({id_placeholders})
    ''', [session_id] + invoice_ids)
    
    removed = result.rowcount
    
    # FIX: Clear allocation flags for removed invoices
    # BUT ONLY if they're not in finalized reports (to preserve dispatch history)
    result = execute_sqlite_wrapper(db, f'''
        UPDATE orders
        SET is_allocated = 0, allocated_date = NULL, manifest_number = NULL
        WHERE id IN ({id_placeholders})
        AND (manifest_number IS NULL OR manifest_number NOT IN (
            SELECT DISTINCT manifest_number FROM reports
        ))
    ''', invoice_ids)
    
    db.commit()
    db.close()
    return removed

def clear_staging(session_id: str) -> int:
    """Clear all staging entries for a session. Returns count cleared."""
    db = SessionLocal()
    
    result = execute_sqlite_wrapper(db, 'DELETE FROM manifest_staging WHERE session_id = ?', (session_id,))
    cleared = result.rowcount
    
    db.commit()
    db.close()
    return cleared

# =============================================
# USER FUNCTIONS
# =============================================

def hash_password(password: str) -> str:
    """Hash a password using SHA-256."""
    return hashlib.sha256(password.encode()).hexdigest()

def get_user(username: str) -> Optional[Dict]:
    """Get a user by username."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'SELECT * FROM users WHERE username = ?', (username,))
    row = result.fetchone()
    db.close()
    return dict(row._mapping) if row else None

def create_user(username: str, password: str, is_admin: bool = False, can_manifest: bool = True) -> bool:
    """Create a new user. Returns True if successful."""
    db = SessionLocal()
    
    try:
        result = execute_sqlite_wrapper(db, '''
            INSERT INTO users (username, password_hash, is_admin, can_manifest, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            username,
            hash_password(password),
            1 if is_admin else 0,
            1 if can_manifest else 0,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))
        db.commit()
        db.close()
        return True
    except IntegrityError:
        db.close()
        return False

def verify_user(username: str, password: str) -> Optional[Dict]:
    """Verify user credentials. Returns user dict if valid, None otherwise."""
    user = get_user(username)
    if not user:
        return None

    stored_hash = user["password_hash"]

    # bcrypt hash
    if stored_hash.startswith("$2"):
        if bcrypt.checkpw(password.encode(), stored_hash.encode()):
            return user
        return None

    # legacy SHA-256
    if hashlib.sha256(password.encode()).hexdigest() == stored_hash:
        return user
    return None

def get_all_users() -> List[Dict]:
    """Get all users (without password hashes)."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'SELECT id, username, is_admin, can_manifest, created_at FROM users')
    rows = result.fetchall()
    db.close()
    return [dict(row._mapping) for row in rows]

def delete_user(username: str) -> bool:
    """Delete a user by username."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'DELETE FROM users WHERE username = ?', (username,))
    deleted = result.rowcount > 0
    db.commit()
    db.close()
    return deleted

def update_user(username: str, password: str = None, is_admin: bool = None, can_manifest: bool = None) -> bool:
    """Update user details."""
    db = SessionLocal()
    
    updates = []
    params = []
    
    if password is not None:
        updates.append('password_hash = ?')
        params.append(hash_password(password))
    if is_admin is not None:
        updates.append('is_admin = ?')
        params.append(1 if is_admin else 0)
    if can_manifest is not None:
        updates.append('can_manifest = ?')
        params.append(1 if can_manifest else 0)
    
    if not updates:
        db.close()
        return False
    
    params.append(username)
    result = execute_sqlite_wrapper(db, f'UPDATE users SET {", ".join(updates)} WHERE username = ?', params)
    updated = result.rowcount > 0
    db.commit()
    db.close()
    return updated

# =============================================
# REPORT FUNCTIONS
# =============================================

def save_report(report_data: Dict) -> int:
    """Save a dispatch report. Returns the report ID."""
    db = SessionLocal()
    
    result = execute_sqlite_wrapper(db, '''
        INSERT INTO reports (manifest_number, date, date_dispatched, driver, assistant, checker, reg_number,
                            pallets_brown, pallets_blue, crates, mileage, total_value, 
                            total_sku, total_weight, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        report_data.get('manifestNumber'),
        report_data.get('date'),
        report_data.get('date'),  # date_dispatched = date
        report_data.get('driver'),
        report_data.get('assistant'),
        report_data.get('checker'),
        report_data.get('regNumber'),
        report_data.get('palletsBrown', 0),
        report_data.get('palletsBlue', 0),
        report_data.get('crates', 0),
        report_data.get('mileage', 0),
        report_data.get('totalValue', 0),
        report_data.get('totalSku', 0),
        report_data.get('totalWeight', 0),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))
    
    report_id = result.inserted_primary_key[0] if getattr(result, 'inserted_primary_key', None) else None
    
    # Save report items (invoices)
    for invoice in report_data.get('invoices', []):
        result = execute_sqlite_wrapper(db, '''
            INSERT INTO report_items (report_id, invoice_number, order_number, customer_name,
                                        invoice_date, area, sku, value, weight, customer_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            report_id,
            invoice.get('num') or invoice.get('invoice_number', 'N/A'),
            invoice.get('orderNum') or invoice.get('order_number', 'N/A'),
            invoice.get('customer') or invoice.get('customer_name', 'N/A'),
            invoice.get('invoiceDate') or invoice.get('invoice_date', 'N/A'),
            invoice.get('area', 'UNKNOWN'),
            invoice.get('sku', 0),
            invoice.get('value', 0) or invoice.get('total_value', 0),
            invoice.get('weight', 0),
            invoice.get('customerNumber') or invoice.get('customer_number', 'N/A')
        ))
    
    # FIX: Finalize invoices from staging if session_id is provided
    session_id = report_data.get('session_id')
    if session_id:
        # Get all invoice filenames from staging for this session
        result = execute_sqlite_wrapper(db, '''
            SELECT o.filename
            FROM orders o
            INNER JOIN manifest_staging ms ON ms.invoice_id = o.id
            WHERE ms.session_id = ?
        ''', (session_id,))
        
        staged_filenames = [row['filename'] for row in result.fetchall()]
        
        if staged_filenames:
            # Update invoices to DISPATCHED
            placeholders = ','.join(['?' for _ in staged_filenames])
            allocated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            result = execute_sqlite_wrapper(db, f'''
                UPDATE orders
                SET is_allocated = 1, allocated_date = ?, manifest_number = ?
                WHERE filename IN ({placeholders})
            ''', [allocated_date, report_data.get('manifestNumber')] + staged_filenames)
            
            print(f"Finalized {result.rowcount} invoices from staging for session: {session_id}")
        
        # Clear staging for this session
        result = execute_sqlite_wrapper(db, 'DELETE FROM manifest_staging WHERE session_id = ?', (session_id,))
        print(f"Cleared staging for session: {session_id}")
    
    db.commit()
    db.close()

    # Log the event
    log_manifest_event(report_data.get('manifestNumber'), 'CREATED', 'System')

    return report_id

def get_reports(date_from: str = None, date_to: str = None) -> List[Dict]:
    """Get all reports with their items, optionally filtered by date range."""
    db = SessionLocal()
    
    query = 'SELECT * FROM reports'
    params = []
    
    if date_from or date_to:
        conditions = []
        if date_from:
            conditions.append('date >= ?')
            params.append(date_from)
        if date_to:
            conditions.append('date <= ?')
            # Extend date_to to end of day to include all dispatches on that date
            params.append(date_to + " 23:59:59")
        query += ' WHERE ' + ' AND '.join(conditions)
    
    query += ' ORDER BY id DESC'
    result = execute_sqlite_wrapper(db, query, params)
    reports = [dict(row) for row in result.fetchall()]
    
    # Add items to each report
    for report in reports:
        result = execute_sqlite_wrapper(db, 'SELECT * FROM report_items WHERE report_id = ?', (report['id'],))
        report['invoices'] = [dict(row) for row in result.fetchall()]
    
    db.close()
    return reports

def get_manifest_details(manifest_number: str) -> Optional[Dict]:
    """Get full details of a specific manifest including invoices and events."""
    db = SessionLocal()
    
    # Get Report Metadata
    result = execute_sqlite_wrapper(db, 'SELECT * FROM reports WHERE manifest_number = ?', (manifest_number,))
    report = result.fetchone()
    
    if not report:
        db.close()
        return None
        
    result = dict(report)
    
    # Get Linked Invoices
    result = execute_sqlite_wrapper(db, 'SELECT * FROM report_items WHERE report_id = ?', (result['id'],))
    result['invoices'] = [dict(row) for row in result.fetchall()]
    
    # Get Audit Events
    result = execute_sqlite_wrapper(db, 'SELECT * FROM manifest_events WHERE manifest_number = ? ORDER BY timestamp DESC', (manifest_number,))
    result['events'] = [dict(row) for row in result.fetchall()]
    
    db.close()
    return result

def log_manifest_event(manifest_number: str, event_type: str, performed_by: str = 'System') -> bool:
    """Log an event for a manifest."""
    db = SessionLocal()
    try:
        result = execute_sqlite_wrapper(db, '''
            INSERT INTO manifest_events (manifest_number, event_type, performed_by, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (manifest_number, event_type, performed_by, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        db.commit()
        db.close()
        return True
    except Exception as e:
        print(f"Error logging event: {e}")
        db.close()
        return False

# =============================================
# SETTINGS FUNCTIONS
# =============================================

def get_settings(category: str) -> List[str]:
    """Get all values for a settings category (drivers, assistants, checkers, routes)."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'SELECT value FROM settings WHERE category = ? ORDER BY value', (category,))
    rows = result.fetchall()
    db.close()
    return [row._mapping['value'] for row in rows]

def add_setting(category: str, value: str) -> bool:
    """Add a setting value."""
    db = SessionLocal()
    try:
        result = execute_sqlite_wrapper(db, 'INSERT INTO settings (category, value) VALUES (?, ?)', (category, value))
        db.commit()
        db.close()
        return True
    except IntegrityError:
        db.close()
        return False

def delete_setting(category: str, value: str) -> bool:
    """Delete a setting value."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'DELETE FROM settings WHERE category = ? AND value = ?', (category, value))
    deleted = result.rowcount > 0
    db.commit()
    db.close()
    return deleted

def update_setting(category: str, old_value: str, new_value: str) -> bool:
    """Update a setting value."""
    db = SessionLocal()
    try:
        result = execute_sqlite_wrapper(db, 'UPDATE settings SET value = ? WHERE category = ? AND value = ?', 
                      (new_value, category, old_value))
        updated = result.rowcount > 0
        db.commit()
        db.close()
        return updated
    except IntegrityError:
        db.close()
        return False

# =============================================
# TRUCK FUNCTIONS
# =============================================

def get_trucks() -> List[Dict]:
    """Get all trucks."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'SELECT * FROM trucks ORDER BY reg')
    rows = result.fetchall()
    db.close()
    return [dict(row._mapping) for row in rows]

def add_truck(reg: str, driver: str = None, assistant: str = None, checker: str = None) -> bool:
    """Add a truck."""
    db = SessionLocal()
    try:
        result = execute_sqlite_wrapper(db, 'INSERT INTO trucks (reg, driver, assistant, checker) VALUES (?, ?, ?, ?)',
                      (reg, driver, assistant, checker))
        db.commit()
        db.close()
        return True
    except IntegrityError:
        db.close()
        return False

def delete_truck(reg: str) -> bool:
    """Delete a truck by registration."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'DELETE FROM trucks WHERE reg = ?', (reg,))
    deleted = result.rowcount > 0
    db.commit()
    db.close()
    return deleted

def update_truck(reg: str, driver: str = None, assistant: str = None, checker: str = None) -> bool:
    """Update truck details."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'UPDATE trucks SET driver = ?, assistant = ?, checker = ? WHERE reg = ?',
                  (driver, assistant, checker, reg))
    updated = result.rowcount > 0
    db.commit()
    db.close()
    return updated

# =============================================
# CUSTOMER ROUTE FUNCTIONS
# =============================================

def get_customer_routes() -> Dict[str, str]:
    """Get all customer route mappings."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'SELECT customer_name, route_name FROM customer_routes')
    rows = result.fetchall()
    db.close()
    return {row._mapping['customer_name']: row._mapping['route_name'] for row in rows}

def add_customer_route(customer_name: str, route_name: str) -> bool:
    """Add or update a customer route mapping."""
    db = SessionLocal()
    try:
        # Use REPLACE to handle updates
        result = execute_sqlite_wrapper(db, 'REPLACE INTO customer_routes (customer_name, route_name) VALUES (?, ?)', 
                      (customer_name, route_name))
        db.commit()
        db.close()
        return True
    except IntegrityError:
        db.close()
        return False

def delete_customer_route(customer_name: str) -> bool:
    """Delete a customer route mapping."""
    db = SessionLocal()
    result = execute_sqlite_wrapper(db, 'DELETE FROM customer_routes WHERE customer_name = ?', (customer_name,))
    deleted = result.rowcount > 0
    db.commit()
    db.close()
    return deleted

# =============================================
# MIGRATION HELPER
# =============================================

def migrate_from_json(json_file_path: str) -> int:
    """Migrate existing orders from JSON file to database. Returns count of migrated orders."""
    if not os.path.exists(json_file_path):
        return 0
    
    try:
        with open(json_file_path, 'r') as f:
            orders = json.load(f)
    except (json.JSONDecodeError, IOError):
        return 0
    
    migrated = 0
    for order in orders:
        if add_order(order):
            migrated += 1
    
    return migrated


# =============================================
# NEW DISPATCH REPORT QUERIES
# =============================================

def get_dispatched_invoices(
    date_from: str = None,
    date_to: str = None,
    filter_type: str = "dispatch",  # NEW: 'dispatch' or 'manifest'
    search_query: str = None,
    limit: int = 50,
    offset: int = 0,
    sort_by: str = "date_dispatched",
    sort_order: str = "DESC"
) -> tuple:
    """
    Get dispatched invoices as INVOICE-LEVEL ROWS (one row per invoice).
    
    Each row includes denormalized dispatch metadata + invoice snapshot.
    
    Args:
        date_from: Filter by date >= this value (YYYY-MM-DD)
        date_to: Filter by date <= this value (YYYY-MM-DD)
        filter_type: 'dispatch' (filter by r.date_dispatched) or 'manifest' (filter by manifest creation)
        search_query: Global text search across invoice #, order #, manifest #, customer, driver, truck reg, checker
        limit: Maximum number of results to return (pagination)
        offset: Number of results to skip (pagination)
        sort_by: Field to sort by (default: date_dispatched)
        sort_order: Sort order ASC or DESC (default: DESC)
    
    Returns:
        Tuple of (results list, total count after filters)
    """
    db = SessionLocal()
    
    # Base query - JOIN reports and report_items to get invoice-level rows
    query = """
        SELECT 
            r.manifest_number,
            r.date_dispatched,
            r.driver,
            r.assistant,
            r.checker,
            r.reg_number,
            ri.invoice_number,
            ri.order_number,
            ri.customer_name,
            ri.customer_number,
            ri.invoice_date,
            ri.area,
            ri.sku,
            ri.value,
            ri.weight
        FROM reports r
        INNER JOIN report_items ri ON r.id = ri.report_id
    """
    
    where_clauses = []
    params = []
    
    # Date range filtering based on filter_type
    # NOTE: Currently both modes use r.date_dispatched because we don't have
    # a timestamp on report_items. This can be enhanced later.
    if filter_type == "manifest":
        # Future enhancement: filter by report_items.created_at when that column exists
        # For now, use same logic as dispatch mode
        if date_from:
            where_clauses.append("r.date_dispatched >= ?")
            params.append(date_from)
        
        if date_to:
            where_clauses.append("r.date_dispatched <= ?")
            params.append(date_to)
    else:  # dispatch mode (default)
        if date_from:
            where_clauses.append("r.date_dispatched >= ?")
            params.append(date_from)
        
        if date_to:
            where_clauses.append("r.date_dispatched <= ?")
            params.append(date_to)
    
    # Global text search
    if search_query:
        search_pattern = f"%{search_query}%"
        where_clauses.append("""(
            ri.invoice_number LIKE ? OR
            ri.order_number LIKE ? OR
            r.manifest_number LIKE ? OR
            ri.customer_name LIKE ? OR
            r.driver LIKE ? OR
            r.reg_number LIKE ? OR
            r.checker LIKE ?
        )""")
        # Add the search pattern 7 times for each field
        params.extend([search_pattern] * 7)
    
    # Add WHERE clause if any filters
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    # Get total count (before pagination)
    count_query = f"SELECT COUNT(*) FROM ({query})"
    result = execute_sqlite_wrapper(db, count_query, params)
    total_count = result.fetchone()[0]
    
    # Add sorting
    valid_sort_fields = {
        "date_dispatched": "r.date_dispatched",
        "manifest_number": "r.manifest_number",
        "invoice_number": "ri.invoice_number",
        "customer_name": "ri.customer_name",
        "driver": "r.driver"
    }
    
    sort_field = valid_sort_fields.get(sort_by, "r.date_dispatched")
    sort_order = "DESC" if sort_order.upper() == "DESC" else "ASC"
    query += f" ORDER BY {sort_field} {sort_order}"
    
    # Add pagination
    query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    # Execute query
    result = execute_sqlite_wrapper(db, query, params)
    rows = result.fetchall()
    db.close()
    
    # Convert to list of dicts
    results = []
    for row in rows:
        results.append({
            "manifest_number": row[0],
            "date_dispatched": row[1],
            "driver": row[2],
            "assistant": row[3],
            "checker": row[4],
            "reg_number": row[5],
            "invoice_number": row[6],
            "order_number": row[7],
            "customer_name": row[8],
            "customer_number": row[9],
            "invoice_date": row[10],
            "area": row[11],
            "sku": row[12],
            "value": row[13],
            "weight": row[14]
        })
    
    return (results, total_count)


def get_outstanding_orders() -> List[Dict]:
    """
    Get outstanding orders (invoices with NO dispatch record).
    
    CRITICAL: This checks if invoice_number exists in report_items table,
    NOT the is_allocated flag. This ensures we get truly un-dispatched invoices.
    
    Excludes cancelled invoices.
    
    Returns:
        List of outstanding orders with: invoice_number, order_number, customer_name, invoice_date
    """
    db = SessionLocal()
    
    query = """
        SELECT
            invoice_number,
            order_number,
            customer_name,
            invoice_date,
            customer_number,
            total_value,
            area
        FROM orders o
        WHERE
            NOT EXISTS (
                SELECT 1 FROM report_items ri
                WHERE ri.invoice_number = o.invoice_number
            )
            AND o.status != 'CANCELLED'
            AND o.type = 'INVOICE'
        ORDER BY o.invoice_date DESC, o.invoice_number DESC
    """
    
    result = execute_sqlite_wrapper(db, query)
    rows = result.fetchall()
    db.close()
    
    # Convert to list of dicts
    results = []
    for row in rows:
        results.append({
            "invoice_number": row[0],
            "order_number": row[1],
            "customer_name": row[2],
            "invoice_date": row[3],
            "customer_number": row[4],
            "total_value": row[5],
            "area": row[6]
        })
    
    return results


# Initialize DB when module is imported
if __name__ == "__main__":
    init_db()
    # Create default admin user if no users exist
    if not get_all_users():
        create_user('admin', 'admin', is_admin=True, can_manifest=True)
        print("Created default admin user (admin/admin)")

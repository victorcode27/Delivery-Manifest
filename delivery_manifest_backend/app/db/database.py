"""
app/db/database.py

SQLAlchemy engine, session factory, and declarative base.

This module is the single source of truth for database connectivity.
All other modules that need a DB session should use:

    from app.db.database import get_db           # FastAPI Depends()  ← preferred
    from app.db.database import get_session      # alias for get_db()
    from app.db.database import get_db_session   # scripts / background tasks
"""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker

from delivery_manifest_backend.app.core.config import settings
from delivery_manifest_backend.app.core.logger import get_logger

logger = get_logger(__name__)

# ── Engine ────────────────────────────────────────────────────────────────────
connect_args: dict = {}
_is_sqlite = settings.DATABASE_URL.startswith("sqlite")
if _is_sqlite:
    connect_args = {"check_same_thread": False}

# Pool settings only apply to real connection pools (PostgreSQL).
# SQLite uses a StaticPool / NullPool internally and ignores these.
_pool_kwargs: dict = {"pool_pre_ping": True}
if not _is_sqlite:
    _pool_kwargs.update({
        "pool_size":     5,     # sustained concurrent connections
        "max_overflow":  10,    # burst headroom (total max = 15)
        "pool_recycle":  1800,  # replace connections every 30 min —
                                # Render.com drops idle connections after ~5 min
        "pool_timeout":  10,    # raise after 10 s if no connection is free
    })

engine = create_engine(
    settings.DATABASE_URL,
    connect_args=connect_args,
    **_pool_kwargs,
)

# ── Session factory ───────────────────────────────────────────────────────────
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ── Declarative base (shared by all ORM models) ───────────────────────────────
Base = declarative_base()


# ── Dependency injection helper (FastAPI) ─────────────────────────────────────

def get_db():
    """
    FastAPI dependency that yields a scoped DB session and guarantees cleanup.

    This is the **preferred** name — it matches the FastAPI / SQLAlchemy docs
    convention so copy-pasted examples work without renaming.

    Usage in a route::

        from sqlalchemy.orm import Session
        from fastapi import Depends
        from app.db.database import get_db

        @router.get("/example")
        def example(db: Session = Depends(get_db)):
            results = db.execute(text("SELECT 1"))
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Alias kept for backward-compatibility with any code written before the rename
get_session = get_db


# ── Manual session helper (scripts / background tasks) ────────────────────────

def get_db_session():
    """
    Return a raw session for use *outside* FastAPI (e.g. migration scripts,
    background threads).

    Caller is responsible for commit / rollback / close::

        db = get_db_session()
        try:
            db.execute(...)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
    """
    return SessionLocal()


# ── SQL helper ────────────────────────────────────────────────────────────────

def execute_query(db, query: str, params=None):
    """
    Execute a raw SQL string via SQLAlchemy, converting legacy SQLite ``?``
    placeholders to named ``:p0`` parameters so the same query strings work
    against PostgreSQL.

    Also auto-appends ``RETURNING id`` to bare INSERT statements so that
    ``result.inserted_primary_key`` is always populated.
    """
    # ── REPLACE INTO → INSERT … ON CONFLICT ──────────────────────────────────
    if "REPLACE INTO" in query.upper():
        query = query.replace("REPLACE INTO", "INSERT INTO")
        if "customer_routes" in query:
            query += (
                " ON CONFLICT (customer_name) "
                "DO UPDATE SET route_name = EXCLUDED.route_name"
            )

    # ── ? → :p0, :p1, … ──────────────────────────────────────────────────────
    if params:
        if "?" in query:
            parts = query.split("?")
            new_query = parts[0]
            named_params: dict = {}
            for idx, val in enumerate(params):
                param_name = f"p{idx}"
                new_query += f":{param_name}" + parts[idx + 1]
                named_params[param_name] = val
            query = new_query
            params = named_params
    else:
        params = {}

    # ── Auto-add RETURNING id to INSERT ──────────────────────────────────────
    query_upper = query.strip().upper()
    if query_upper.startswith("INSERT ") and "RETURNING" not in query_upper:
        query = query.rstrip("; \t\n\r") + " RETURNING id"

    return db.execute(text(query), params)


# ── Schema initialisation ─────────────────────────────────────────────────────

def init_db() -> None:
    """
    Create all tables if they do not already exist and seed the default
    admin user when the users table is empty.

    Called once from ``app/main.py`` at startup.
    """
    db = get_db_session()

    try:
        _create_tables(db)
        db.commit()
        logger.info("Database schema verified.")
        _seed_admin(db)
    except Exception:
        db.rollback()
        logger.error("Database initialisation failed", exc_info=True)
        raise
    finally:
        db.close()


def _create_tables(db) -> None:
    """Issue CREATE TABLE IF NOT EXISTS DDL for every table."""

    ddl_statements = [
        # ── orders ────────────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS orders (
            id               SERIAL PRIMARY KEY,
            filename         TEXT UNIQUE NOT NULL,
            date_processed   TEXT NOT NULL,
            customer_name    TEXT NOT NULL,
            total_value      TEXT DEFAULT '0.00',
            order_number     TEXT DEFAULT 'N/A',
            invoice_number   TEXT DEFAULT 'N/A',
            invoice_date     TEXT DEFAULT 'N/A',
            area             TEXT DEFAULT 'UNKNOWN',
            is_allocated     INTEGER DEFAULT 0,
            allocated_date   TEXT,
            manifest_number  TEXT,
            type             TEXT DEFAULT 'INVOICE',
            reference_number TEXT,
            original_value   TEXT,
            status           TEXT DEFAULT 'PENDING',
            customer_number  TEXT DEFAULT 'N/A'
        )
        """,
        # ── users ─────────────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS users (
            id            SERIAL PRIMARY KEY,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role          TEXT NOT NULL DEFAULT 'ADMIN'
                          CHECK (role IN ('ADMIN', 'DISPATCH', 'REPORTS_ONLY')),
            is_active     BOOLEAN DEFAULT TRUE,
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            updated_at    TIMESTAMPTZ DEFAULT NOW(),
            -- Legacy columns (preserved for rollback, unused by new code)
            is_admin      INTEGER DEFAULT 0,
            can_manifest  INTEGER DEFAULT 1
        )
        """,
        # ── reports ───────────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS reports (
            id              SERIAL PRIMARY KEY,
            manifest_number TEXT NOT NULL,
            date            TEXT,
            date_dispatched TEXT,
            driver          TEXT,
            assistant       TEXT,
            checker         TEXT,
            reg_number      TEXT,
            pallets_brown   INTEGER DEFAULT 0,
            pallets_blue    INTEGER DEFAULT 0,
            crates          INTEGER DEFAULT 0,
            mileage         INTEGER DEFAULT 0,
            total_value     REAL DEFAULT 0,
            total_sku       INTEGER DEFAULT 0,
            total_weight    REAL DEFAULT 0,
            session_id      TEXT,
            created_at      TEXT
        )
        """,
        # ── report_items ──────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS report_items (
            id              SERIAL PRIMARY KEY,
            report_id       INTEGER NOT NULL,
            invoice_number  TEXT NOT NULL,
            order_number    TEXT,
            customer_name   TEXT,
            customer_number TEXT,
            invoice_date    TEXT,
            area            TEXT,
            sku             INTEGER DEFAULT 0,
            value           REAL DEFAULT 0,
            weight          REAL DEFAULT 0,
            FOREIGN KEY (report_id) REFERENCES reports(id)
        )
        """,
        # ── settings ──────────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS settings (
            id       SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            value    TEXT NOT NULL,
            UNIQUE(category, value)
        )
        """,
        # ── trucks ────────────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS trucks (
            id        SERIAL PRIMARY KEY,
            reg       TEXT UNIQUE NOT NULL,
            driver    TEXT,
            assistant TEXT,
            checker   TEXT
        )
        """,
        # ── customer_routes ───────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS customer_routes (
            id            SERIAL PRIMARY KEY,
            customer_name TEXT UNIQUE NOT NULL,
            route_name    TEXT NOT NULL
        )
        """,
        # ── manifest_events ───────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS manifest_events (
            id              SERIAL PRIMARY KEY,
            manifest_number TEXT NOT NULL,
            event_type      TEXT NOT NULL,
            performed_by    TEXT DEFAULT 'System',
            timestamp       TEXT NOT NULL
        )
        """,
        # ── manifests (uploaded files) ────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS manifests (
            id          SERIAL PRIMARY KEY,
            file_name   TEXT NOT NULL,
            uploaded_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status      TEXT DEFAULT 'PENDING'
        )
        """,
        # ── manifest_staging ──────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS manifest_staging (
            id         SERIAL PRIMARY KEY,
            session_id TEXT NOT NULL,
            invoice_id INTEGER NOT NULL,
            added_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES orders(id)
        )
        """,
        # ── index on staging ──────────────────────────────────────────────────
        """
        CREATE INDEX IF NOT EXISTS idx_staging_session
        ON manifest_staging(session_id)
        """,
    ]

    for ddl in ddl_statements:
        db.execute(text(ddl.strip()))

    # ── Column-level migrations (add columns that may not exist yet) ──────────
    has_duplicates = _run_migrations(db)

    # ── Performance indexes ───────────────────────────────────────────────────
    _create_indexes(db, skip_unique_invoice=(has_duplicates or False))


def _column_exists(db, table: str, column: str) -> bool:
    """
    Check whether *column* exists in *table* without raising an exception.

    Uses information_schema (PostgreSQL) or PRAGMA table_info (SQLite) so
    the check never aborts the current transaction.
    """
    if _is_sqlite:
        result = db.execute(text(f"PRAGMA table_info({table})"))
        return any(row[1] == column for row in result.fetchall())
    else:
        result = db.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name = :t AND column_name = :c"
            ),
            {"t": table, "c": column},
        )
        return result.fetchone() is not None


def _run_migrations(db) -> None:
    """Add columns that were introduced after the initial schema."""
    migrations = [
        ("orders",       "customer_number", "ALTER TABLE orders ADD COLUMN customer_number TEXT DEFAULT 'N/A'"),
        ("report_items", "customer_number", "ALTER TABLE report_items ADD COLUMN customer_number TEXT DEFAULT 'N/A'"),
        # User model v2 — is_active / role replace is_admin / can_manifest flags
        ("users", "is_active", "ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT TRUE"),
        ("users", "role",      "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'ADMIN'"),
        # User model v3 — updated_at timestamp
        ("users", "updated_at", "ALTER TABLE users ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW()"),
    ]
    for table, column, sql in migrations:
        try:
            if not _column_exists(db, table, column):
                db.execute(text(sql))
                logger.info(f"Migration applied: {sql}")
        except Exception as exc:
            logger.warning(f"Migration warning ({table}.{column}): {exc}")

    # ── Role CHECK constraint migration ───────────────────────────────────
    # Order is critical:
    #   1. Drop the old constraint first — otherwise writing 'ADMIN' in step 2
    #      is rejected by CHECK (role IN ('FULL_ACCESS', 'REPORTS_ONLY')).
    #   2. Fix all role data (legacy flags + FULL_ACCESS → ADMIN rename).
    #   3. Add the new constraint once the data is guaranteed clean.
    _drop_old_role_constraint(db)
    _migrate_user_roles(db)
    _add_new_role_constraint(db)

    # ── Check for duplicate invoice_numbers (log only — never auto-delete) ──
    # Returns True when duplicates exist → unique index must be deferred.
    return _check_duplicate_invoices(db)


def _create_indexes(db, *, skip_unique_invoice: bool = False) -> None:
    """
    Create performance indexes using IF NOT EXISTS — safe to re-run on every
    startup.  Skips silently if an index already exists or the table is not yet
    present (e.g. during a fresh SQLite test run).

    Indexes are chosen for columns used in WHERE, JOIN, ORDER BY, or subqueries
    across the manifest and invoice flows.  Existing UNIQUE / PK indexes are not
    duplicated.

    When *skip_unique_invoice* is True the partial UNIQUE index on
    ``orders(invoice_number)`` is **not** created — this happens when startup
    detected duplicate invoice_number rows that must be cleaned up manually
    first.
    """
    indexes = [
        # orders — every list query filters on these columns
        ("idx_orders_is_allocated",
         "CREATE INDEX IF NOT EXISTS idx_orders_is_allocated    ON orders(is_allocated)"),
        ("idx_orders_type",
         "CREATE INDEX IF NOT EXISTS idx_orders_type            ON orders(type)"),
        ("idx_orders_status",
         "CREATE INDEX IF NOT EXISTS idx_orders_status          ON orders(status)"),
        # orders.invoice_number — point lookups, NOT EXISTS subquery, LIKE search
        ("idx_orders_invoice_number",
         "CREATE INDEX IF NOT EXISTS idx_orders_invoice_number  ON orders(invoice_number)"),
        # orders.manifest_number — get_current_manifest, NOT EXISTS subquery
        ("idx_orders_manifest_number",
         "CREATE INDEX IF NOT EXISTS idx_orders_manifest_number ON orders(manifest_number)"),
        # orders.date_processed — ORDER BY on every paginated list
        ("idx_orders_date_processed",
         "CREATE INDEX IF NOT EXISTS idx_orders_date_processed  ON orders(date_processed)"),
        # report_items.report_id — FK join in get_manifest_details + bulk fetch
        ("idx_report_items_report_id",
         "CREATE INDEX IF NOT EXISTS idx_report_items_report_id      ON report_items(report_id)"),
        # report_items.invoice_number — NOT EXISTS outstanding query + LIKE search
        ("idx_report_items_invoice_number",
         "CREATE INDEX IF NOT EXISTS idx_report_items_invoice_number ON report_items(invoice_number)"),
        # reports.manifest_number — primary lookup key for manifest details
        ("idx_reports_manifest_number",
         "CREATE INDEX IF NOT EXISTS idx_reports_manifest_number ON reports(manifest_number)"),
        # reports.date_dispatched — date-range filter + ORDER BY in dispatched view
        ("idx_reports_date_dispatched",
         "CREATE INDEX IF NOT EXISTS idx_reports_date_dispatched ON reports(date_dispatched)"),
        # manifest_staging.invoice_id — FK + batch existence check in add_to_staging
        ("idx_staging_invoice_id",
         "CREATE INDEX IF NOT EXISTS idx_staging_invoice_id ON manifest_staging(invoice_id)"),
        # manifest_events.manifest_number — audit log lookup per manifest
        ("idx_events_manifest_number",
         "CREATE INDEX IF NOT EXISTS idx_events_manifest_number ON manifest_events(manifest_number)"),
    ]

    for name, ddl in indexes:
        try:
            db.execute(text(ddl))
        except Exception as exc:
            logger.warning(f"Index creation skipped ({name}): {exc}")

    # ── Partial UNIQUE index on invoice_number ────────────────────────────
    # Only created when the table has no duplicate invoice_numbers.
    # If duplicates exist, _check_duplicate_invoices() logs full detail and
    # this index is deferred until manual cleanup + restart.
    if skip_unique_invoice:
        logger.warning(
            "[Index] Skipping idx_orders_unique_invoice_number — "
            "resolve duplicate invoice_numbers first, then restart."
        )
    else:
        try:
            db.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_unique_invoice_number "
                "ON orders(invoice_number) WHERE invoice_number != 'N/A'"
            ))
        except Exception as exc:
            logger.warning(f"Index creation skipped (idx_orders_unique_invoice_number): {exc}")


def _check_duplicate_invoices(db) -> bool:
    """
    Detect and log duplicate invoice_number rows.  **Never deletes anything.**

    Returns True if duplicates exist (unique index must NOT be created),
    False if the table is clean (safe to create the unique index).

    When duplicates are found the log output includes enough detail for a
    manual cleanup decision:
        invoice_number, id, filename, date_processed, status,
        whether the row is referenced in manifest_staging or report_items.
    """
    try:
        detail = db.execute(text("""
            SELECT o.invoice_number, o.id, o.filename, o.date_processed,
                   o.status,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM manifest_staging ms WHERE ms.invoice_id = o.id
                   ) THEN 'STAGING' ELSE '' END  AS in_staging,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM report_items ri
                       WHERE ri.invoice_number = o.invoice_number
                   ) THEN 'REPORTED' ELSE '' END  AS in_reports
            FROM orders o
            WHERE o.invoice_number IN (
                SELECT invoice_number
                FROM orders
                WHERE invoice_number != 'N/A'
                GROUP BY invoice_number
                HAVING COUNT(*) > 1
            )
            ORDER BY o.invoice_number, o.id
        """)).fetchall()

        if not detail:
            logger.info("[Dedup] No duplicate invoice_numbers found — table is clean.")
            return False

        # ── Count distinct duplicate groups ──
        dup_invoices = set()
        for row in detail:
            dup_invoices.add(row[0])

        logger.warning(
            f"[Dedup] {len(dup_invoices)} duplicate invoice_number group(s) found "
            f"({len(detail)} total rows).  UNIQUE INDEX WILL NOT BE CREATED until "
            f"duplicates are resolved manually."
        )
        logger.warning("[Dedup] Duplicate detail (review before cleanup):")
        for row in detail:
            logger.warning(
                f"  invoice={row[0]}  id={row[1]}  file={row[2]}  "
                f"processed={row[3]}  status={row[4]}  {row[5]}  {row[6]}"
            )
        logger.warning(
            "[Dedup] Run the manual cleanup SQL documented in the migration notes, "
            "then restart the application to create the unique index."
        )
        return True

    except Exception as exc:
        logger.warning(f"[Dedup] Error checking for duplicates: {exc}")
        return True  # assume dirty — do not create unique index


def _migrate_user_roles(db) -> None:
    """
    Idempotent role migration run on every startup.

    Pass 1 – legacy flags → role enum (for rows that pre-date the role column):
      • is_admin = 1                        → ADMIN
      • is_admin = 0  AND can_manifest = 0  → REPORTS_ONLY
      • is_admin = 0  AND can_manifest = 1  → ADMIN
      • anything else / NULL                → ADMIN

    Pass 2 – rename FULL_ACCESS → ADMIN (dropped in this RBAC migration):
      Converts any existing 'FULL_ACCESS' rows to 'ADMIN' so the new CHECK
      constraint is satisfied.  Safe to re-run; REPORTS_ONLY and the new
      DISPATCH rows are untouched.
    """
    try:
        # Pass 1: convert old is_admin/can_manifest flags (only if column exists)
        if _column_exists(db, "users", "can_manifest"):
            db.execute(text(
                "UPDATE users SET role = 'REPORTS_ONLY' "
                "WHERE role NOT IN ('ADMIN', 'DISPATCH', 'REPORTS_ONLY', 'FULL_ACCESS') "
                "AND COALESCE(can_manifest, 1) = 0 "
                "AND COALESCE(is_admin, 0) = 0"
            ))
            db.execute(text(
                "UPDATE users SET role = 'ADMIN' "
                "WHERE role NOT IN ('ADMIN', 'DISPATCH', 'REPORTS_ONLY', 'FULL_ACCESS')"
            ))

        # Pass 2: rename FULL_ACCESS → ADMIN
        db.execute(text(
            "UPDATE users SET role = 'ADMIN' WHERE role = 'FULL_ACCESS'"
        ))

        logger.info("User role data migration completed.")
    except Exception as exc:
        logger.warning(f"User role migration warning: {exc}")


def _drop_old_role_constraint(db) -> None:
    """
    Remove the old users.role CHECK constraint (FULL_ACCESS | REPORTS_ONLY) so
    that subsequent data migrations can safely write 'ADMIN' or 'DISPATCH'.

    PostgreSQL
    ----------
    Inspects pg_constraint for a CHECK constraint on users that still references
    'FULL_ACCESS' in its definition and drops it with ALTER TABLE … DROP CONSTRAINT.
    Idempotent — skips silently if the old constraint is already gone.

    SQLite
    ------
    SQLite does not support ALTER TABLE … DROP CONSTRAINT.  The full fix is done
    here in one shot via table recreation (new table with correct constraint +
    data copy with FULL_ACCESS → ADMIN mapping + swap).  After this runs,
    _migrate_user_roles() and _add_new_role_constraint() are safe no-ops for
    SQLite.
    """
    if _is_sqlite:
        _migrate_role_constraint_sqlite(db)
        return

    try:
        result = db.execute(text("""
            SELECT c.conname
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            WHERE t.relname = 'users'
              AND c.contype = 'c'
              AND pg_get_constraintdef(c.oid) LIKE '%FULL_ACCESS%'
        """))
        row = result.fetchone()
        if row:
            logger.info(f"Dropping old role CHECK constraint '{row[0]}' from users table...")
            db.execute(text(f"ALTER TABLE users DROP CONSTRAINT IF EXISTS {row[0]}"))
    except Exception as exc:
        logger.warning(f"Drop old role constraint warning: {exc}")


def _migrate_role_constraint_sqlite(db) -> None:
    """
    SQLite-specific role constraint migration via table recreation.

    SQLite offers no ALTER TABLE … DROP/ADD CONSTRAINT, so the only safe method
    is the officially recommended pattern:
      1. Create a replacement table with the new constraint.
      2. Copy all rows, mapping FULL_ACCESS (and any other unknown value) → ADMIN.
      3. Drop the old table, rename the replacement.

    Foreign key enforcement is temporarily disabled during the swap.
    Idempotent: checks sqlite_master for 'FULL_ACCESS' in the schema string and
    skips if already migrated.
    """
    try:
        row = db.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='users'"
        )).fetchone()
        if not row or "FULL_ACCESS" not in (row[0] or ""):
            return  # already migrated or table does not exist

        logger.info("SQLite: recreating users table to fix role CHECK constraint...")

        # Discover which columns exist — varies by migration state of the DB.
        cols = {r[1] for r in db.execute(text("PRAGMA table_info(users)")).fetchall()}
        sa = lambda c, fb: c if c in cols else fb  # safe column accessor

        db.execute(text("PRAGMA foreign_keys = OFF"))

        # Replacement table carries the correct constraint.
        db.execute(text("""
            CREATE TABLE _users_v3 (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'ADMIN'
                              CHECK (role IN ('ADMIN', 'DISPATCH', 'REPORTS_ONLY')),
                is_active     BOOLEAN DEFAULT TRUE,
                created_at    TEXT,
                updated_at    TEXT,
                is_admin      INTEGER DEFAULT 0,
                can_manifest  INTEGER DEFAULT 1
            )
        """))

        # Copy rows; any role not in the new allowed set maps to 'ADMIN'.
        db.execute(text(f"""
            INSERT INTO _users_v3
                (id, username, password_hash, role, is_active,
                 created_at, updated_at, is_admin, can_manifest)
            SELECT
                id,
                username,
                password_hash,
                CASE WHEN role IN ('ADMIN', 'DISPATCH', 'REPORTS_ONLY')
                     THEN role ELSE 'ADMIN' END,
                {sa('is_active', '1')},
                {sa('created_at', 'CURRENT_TIMESTAMP')},
                {sa('updated_at', sa('created_at', 'CURRENT_TIMESTAMP'))},
                {sa('is_admin', '0')},
                {sa('can_manifest', '1')}
            FROM users
        """))

        db.execute(text("DROP TABLE users"))
        db.execute(text("ALTER TABLE _users_v3 RENAME TO users"))
        db.execute(text("PRAGMA foreign_keys = ON"))

        logger.info("SQLite users table recreated with new role CHECK constraint.")
    except Exception as exc:
        logger.warning(f"SQLite role constraint migration error: {exc}")
        try:
            db.execute(text("PRAGMA foreign_keys = ON"))
        except Exception:
            pass


def _add_new_role_constraint(db) -> None:
    """
    Add the new users.role CHECK constraint (ADMIN | DISPATCH | REPORTS_ONLY)
    after data has been cleaned up by _migrate_user_roles().

    PostgreSQL
    ----------
    Checks pg_constraint for an existing constraint that references 'ADMIN'.
    If none is found, verifies that every row holds a valid role value, then
    issues ALTER TABLE … ADD CONSTRAINT.  Idempotent.

    SQLite
    ------
    No-op — the new constraint was already embedded in the replacement table
    created by _migrate_role_constraint_sqlite() in _drop_old_role_constraint().
    """
    if _is_sqlite:
        return  # handled by _migrate_role_constraint_sqlite

    try:
        # Skip if the new constraint is already present.
        result = db.execute(text("""
            SELECT 1
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            WHERE t.relname = 'users'
              AND c.contype = 'c'
              AND pg_get_constraintdef(c.oid) LIKE '%ADMIN%'
        """))
        if result.fetchone():
            return  # already migrated

        # Verify data is clean before adding the constraint; if any row would
        # violate it PostgreSQL would reject the ADD CONSTRAINT entirely.
        bad = db.execute(text(
            "SELECT COUNT(*) FROM users "
            "WHERE role NOT IN ('ADMIN', 'DISPATCH', 'REPORTS_ONLY')"
        )).scalar()
        if bad:
            logger.warning(
                f"Cannot add role CHECK constraint: {bad} row(s) still carry "
                f"invalid role values.  Data migration may not have completed."
            )
            return

        db.execute(text(
            "ALTER TABLE users "
            "ADD CONSTRAINT users_role_check "
            "CHECK (role IN ('ADMIN', 'DISPATCH', 'REPORTS_ONLY'))"
        ))
        logger.info("Added new role CHECK constraint to users table.")
    except Exception as exc:
        logger.warning(f"Add new role constraint warning: {exc}")


def _seed_admin(db) -> None:
    """Create the default admin user if no users exist."""
    from delivery_manifest_backend.app.core.security import hash_password

    result = db.execute(text("SELECT COUNT(*) FROM users"))
    if result.scalar() == 0:
        db.execute(
            text(
                "INSERT INTO users (username, password_hash, role, is_active, is_admin, can_manifest, created_at) "
                "VALUES (:u, :p, 'ADMIN', TRUE, 1, 1, NOW())"
            ),
            {"u": "admin", "p": hash_password("admin")},
        )
        db.commit()
        logger.info("Default admin user created (admin / admin).")

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

from app.core.config import settings
from app.core.logger import get_logger

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
            is_admin      INTEGER DEFAULT 0,
            can_manifest  INTEGER DEFAULT 1,
            created_at    TEXT
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
    _run_migrations(db)

    # ── Performance indexes ───────────────────────────────────────────────────
    _create_indexes(db)


def _run_migrations(db) -> None:
    """Add columns that were introduced after the initial schema."""
    migrations = [
        ("orders",       "customer_number", "ALTER TABLE orders ADD COLUMN customer_number TEXT DEFAULT 'N/A'"),
        ("report_items", "customer_number", "ALTER TABLE report_items ADD COLUMN customer_number TEXT DEFAULT 'N/A'"),
        # User model v2 — is_active / role replace is_admin / can_manifest flags
        ("users", "is_active", "ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT TRUE"),
        ("users", "role",      "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user'"),
    ]
    for table, column, sql in migrations:
        try:
            db.execute(text(f"SELECT {column} FROM {table} LIMIT 1"))
        except OperationalError:
            try:
                db.execute(text(sql))
                logger.info(f"Migration applied: {sql}")
            except Exception as exc:
                logger.warning(f"Migration warning ({table}.{column}): {exc}")


def _create_indexes(db) -> None:
    """
    Create performance indexes using IF NOT EXISTS — safe to re-run on every
    startup.  Skips silently if an index already exists or the table is not yet
    present (e.g. during a fresh SQLite test run).

    Indexes are chosen for columns used in WHERE, JOIN, ORDER BY, or subqueries
    across the manifest and invoice flows.  Existing UNIQUE / PK indexes are not
    duplicated.
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


def _seed_admin(db) -> None:
    """Create the default admin/admin user if no users exist."""
    from app.core.security import hash_password
    from datetime import datetime

    result = db.execute(text("SELECT COUNT(*) FROM users"))
    if result.scalar() == 0:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db.execute(
            text(
                "INSERT INTO users (username, password_hash, is_admin, can_manifest, created_at) "
                "VALUES (:u, :p, :a, :c, :n)"
            ),
            {"u": "admin", "p": hash_password("admin"), "a": 1, "c": 1, "n": now},
        )
        db.commit()
        logger.info("Default admin user created (admin / admin).")

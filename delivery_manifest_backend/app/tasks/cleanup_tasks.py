"""
app/tasks/cleanup_tasks.py

Background task: automatic stale invoice cleanup.

Runs once at application startup, then once every 24 hours.

Rule:
    Cancel all invoices where:
        status  = 'PENDING'
        AND invoice_date is older than 12 months
        AND invoice_date is not NULL or 'N/A'

Only sets status = 'CANCELLED' — no records are deleted.
Processes in batches of 500 to avoid long-running transactions on large tables.

Usage (called from app/main.py)::

    from app.tasks.cleanup_tasks import start_cleanup, stop_cleanup
"""

import threading
import time
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import text

from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.db.database import get_db_session

logger = get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

CLEANUP_INTERVAL_SECONDS = 86_400   # 24 hours
BATCH_SIZE               = 500
STALE_MONTHS             = 12


# ── Cleanup logic ─────────────────────────────────────────────────────────────

def _cutoff_date() -> str:
    """Return the cutoff date string (today minus STALE_MONTHS) as 'YYYY-MM-DD'."""
    today   = date.today()
    # Subtract 12 months: step back one year, handling month/year boundaries cleanly.
    year    = today.year - (STALE_MONTHS // 12)
    month   = today.month - (STALE_MONTHS % 12)
    if month <= 0:
        month += 12
        year  -= 1
    # Clamp day to valid range (e.g. Feb 29 → Feb 28 on non-leap years).
    import calendar
    max_day = calendar.monthrange(year, month)[1]
    day     = min(today.day, max_day)
    return date(year, month, day).isoformat()


def run_cleanup() -> int:
    """
    Cancel all stale PENDING invoices in batches.

    Returns the total number of rows cancelled in this run.
    Exceptions are caught and logged — a failure never crashes the caller.
    """
    cutoff        = _cutoff_date()
    total         = 0
    batch_num     = 0
    db            = get_db_session()

    logger.info(f"[Cleanup] Starting stale invoice cleanup — cutoff date: {cutoff}")

    try:
        while True:
            result = db.execute(
                text("""
                    UPDATE orders
                    SET    status = 'CANCELLED'
                    WHERE  id IN (
                        SELECT id
                        FROM   orders
                        WHERE  status       = 'PENDING'
                        AND    invoice_date IS NOT NULL
                        AND    invoice_date != 'N/A'
                        AND    invoice_date <  :cutoff
                        LIMIT  :batch_size
                    )
                """),
                {"cutoff": cutoff, "batch_size": BATCH_SIZE},
            )
            db.commit()

            rows = result.rowcount
            if rows == 0:
                break

            batch_num += 1
            total     += rows
            logger.info(
                f"[Cleanup] Batch {batch_num}: cancelled {rows} invoices "
                f"(total so far: {total})"
            )

        if total > 0:
            logger.info(f"[Cleanup] Run complete — {total} stale invoices cancelled.")
        else:
            logger.info("[Cleanup] Run complete — no stale invoices found.")

    except Exception:
        db.rollback()
        logger.error("[Cleanup] Error during stale invoice cleanup", exc_info=True)
    finally:
        db.close()

    return total


# ── Background thread ─────────────────────────────────────────────────────────

class StaleInvoiceCleaner:
    """
    Runs run_cleanup() once at startup, then on a 24-hour interval.

    Follows the same pattern as FileWatcher in pod_tasks.py:
    a daemon thread with a running flag for clean shutdown.
    """

    def __init__(self, interval: int = CLEANUP_INTERVAL_SECONDS) -> None:
        self.interval = interval
        self.running  = False

    def run(self) -> None:
        self.running = True
        logger.info(
            f"[Cleanup] Stale invoice cleaner started "
            f"(interval: {self.interval // 3600}h, batch size: {BATCH_SIZE}, "
            f"cutoff: {STALE_MONTHS} months)."
        )

        while self.running:
            run_cleanup()

            # Sleep in 60-second increments so shutdown signals are handled promptly.
            elapsed = 0
            while self.running and elapsed < self.interval:
                time.sleep(60)
                elapsed += 60

        logger.info("[Cleanup] Stale invoice cleaner stopped.")


# ── Public start / stop helpers (called from main.py lifecycle hooks) ─────────

_cleaner_thread:  Optional[threading.Thread]     = None
_cleaner_service: Optional[StaleInvoiceCleaner]  = None


def start_cleanup(interval: int = CLEANUP_INTERVAL_SECONDS) -> StaleInvoiceCleaner:
    """
    Start the StaleInvoiceCleaner in a daemon thread.
    Returns the cleaner instance.
    """
    global _cleaner_thread, _cleaner_service

    _cleaner_service = StaleInvoiceCleaner(interval=interval)
    _cleaner_thread  = threading.Thread(
        target=_cleaner_service.run,
        daemon=True,
        name="stale-invoice-cleaner",
    )
    _cleaner_thread.start()
    logger.info("[Cleanup] Stale invoice cleaner thread started.")
    return _cleaner_service


def stop_cleanup() -> None:
    """Signal the cleaner to stop after its current sleep cycle."""
    global _cleaner_service
    if _cleaner_service:
        logger.info("[Cleanup] Stopping stale invoice cleaner…")
        _cleaner_service.running = False

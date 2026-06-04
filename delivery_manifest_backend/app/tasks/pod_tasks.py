"""
app/tasks/pod_tasks.py

Background tasks for Proof-of-Delivery (POD) integration and the file watcher.

The FileWatcher polls the invoice input folder for new PDFs, checks that each
file has finished writing (size-stable), extracts invoice data, and saves it
to the database.  It runs in a daemon thread started at application startup.

Usage (called from app/main.py)::

    from app.tasks.pod_tasks import start_watcher, stop_watcher
"""

import datetime
import re
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

from delivery_manifest_backend.app.core.config import settings
from delivery_manifest_backend.app.core.logger import get_logger

logger = get_logger(__name__)

# ── Watcher configuration ─────────────────────────────────────────────────────
POLL_INTERVAL       = 20   # seconds between folder scans
STABILITY_CHECKS    = 3    # consecutive size checks before processing
STABILITY_DELAY     = 2    # seconds between stability checks


class FileWatcher:
    """
    Poll a folder for new PDF invoices and submit them to the database.

    The watcher is intentionally self-contained so it can be unit-tested
    without starting the FastAPI server.
    """

    def __init__(
        self,
        watch_folder: str = settings.INVOICE_INPUT_FOLDER,
        poll_interval: int = POLL_INTERVAL,
    ) -> None:
        self.watch_folder  = Path(watch_folder)
        self.poll_interval = poll_interval
        self.running       = False
        self.known_files:  Set[str]       = set()
        self.file_sizes:   Dict[str, int] = {}
        self.last_scan_time: Optional[str] = None
        # Pre-filter constants — overwritten from invoice_processor at init time.
        # Defaults here ensure the watcher is always safe even if the import fails.
        self._cutoff_ts:     float      = datetime.datetime(2026, 6, 1).timestamp()
        self._skip_patterns: List[str]  = [r"REPRINT", r"FISCAL", r"TAX.?INVOICE"]

    # ── File stability ────────────────────────────────────────────────────────

    def is_file_stable(self, file_path: Path) -> bool:
        """Return True when the file has a consistent non-zero size."""
        try:
            if not file_path.exists():
                return False
            previous: Optional[int] = None
            for check in range(STABILITY_CHECKS):
                size = file_path.stat().st_size
                if size == 0:
                    return False
                if previous is not None and size != previous:
                    return False
                previous = size
                if check < STABILITY_CHECKS - 1:
                    time.sleep(STABILITY_DELAY)
            # Final lock-check: try opening the file
            with open(file_path, "rb") as fh:
                fh.read(1024)
            logger.info(f"[STABLE] {file_path.name} ({previous} bytes)")
            return True
        except (PermissionError, OSError):
            return False
        except Exception:
            logger.error(f"Stability check error for {file_path.name}", exc_info=True)
            return False

    # ── Folder scan ───────────────────────────────────────────────────────────

    def scan_folder(self) -> Set[Path]:
        try:
            if not self.watch_folder.exists():
                logger.error(f"Watch folder missing: {self.watch_folder}")
                return set()
            return set(self.watch_folder.glob("*.pdf"))
        except Exception:
            logger.error("Scan error", exc_info=True)
            return set()

    # ── Initialisation ────────────────────────────────────────────────────────

    def _init_known_files(self) -> None:
        """Seed known_files from the DB — only files already imported are 'known'.

        Files present in the folder but NOT yet in the database are left out
        of ``known_files`` so they will be picked up on the next poll cycle.
        This ensures files that arrived while the watcher was down are never
        silently lost — and the DB-level unique index on ``invoice_number``
        guarantees that re-processing a file whose invoice is already stored
        is a safe no-op.
        """
        from delivery_manifest_backend.app.db.database import init_db, get_db_session
        from sqlalchemy import text

        # Sync cutoff and skip-pattern constants from invoice_processor so both
        # import paths always use the same threshold.
        try:
            import invoice_processor  # type: ignore
            self._cutoff_ts     = invoice_processor.IMPORT_CUTOFF_DATE.timestamp()
            self._skip_patterns = invoice_processor.SKIP_FILENAME_PATTERNS
        except ImportError:
            pass  # keep __init__ defaults

        init_db()
        db = get_db_session()
        try:
            result = db.execute(text("SELECT filename FROM orders"))
            processed = {row[0] for row in result.fetchall()}
        finally:
            db.close()

        folder_files = self.scan_folder()
        skipped_old = 0
        for fp in folder_files:
            if fp.name in processed:
                self.known_files.add(fp.name)
                continue
            # Mark files older than the cutoff as known so they are never polled.
            # This prevents the watcher from running stability checks on tens of
            # thousands of historical PDFs that will be rejected by the date gate
            # in _process_file() anyway.
            try:
                if fp.stat().st_mtime < self._cutoff_ts:
                    self.known_files.add(fp.name)
                    skipped_old += 1
                    continue
            except OSError:
                pass
            # Mark excluded filename types (REPRINT, FISCAL, TAX INVOICE) as known.
            if any(re.search(p, fp.name, re.IGNORECASE) for p in self._skip_patterns):
                self.known_files.add(fp.name)

        missed = len(folder_files) - len(self.known_files)
        logger.info(
            f"Watcher initialised: {len(folder_files)} folder PDFs, "
            f"{len(self.known_files)} already known "
            f"({len(processed)} in DB, {skipped_old} pre-cutoff/excluded), "
            f"{missed} new file(s) pending."
        )

    # ── Process a new file ────────────────────────────────────────────────────

    def _process_file(self, file_path: Path) -> bool:
        """Extract invoice data and save it to the database.

        Acquires the shared ``_import_lock`` so this cannot run concurrently
        with ``refresh_invoices()`` (which reloads the invoice_processor module).
        """
        try:
            from delivery_manifest_backend.app.services.manifest_service import (
                _import_lock,
                add_order,
            )

            with _import_lock:
                import invoice_processor  # type: ignore  (lives in the legacy root)

                invoice_data = invoice_processor.extract_invoice_data(str(file_path))
                if not invoice_data:
                    logger.error(f"[SKIP] No data extracted from {file_path.name}")
                    return False

                # --- STRICT CUTOFF DATE FILTER (content-based, not file mtime) ---
                # Strict clean-start from 2026-06-01: missing, unparseable, or
                # pre-cutoff invoice dates are all blocked so no old data enters
                # the live database.
                _cutoff = invoice_processor.IMPORT_CUTOFF_DATE.date()
                inv_date_str = invoice_data.get("invoice_date", "N/A")
                if not inv_date_str or inv_date_str == "N/A":
                    logger.warning(
                        f"[SKIP-DATE-N/A] No invoice date extracted, "
                        f"not importing: {file_path.name}"
                    )
                    return False
                try:
                    inv_dt = datetime.date.fromisoformat(inv_date_str)
                    if inv_dt < _cutoff:
                        logger.info(
                            f"[SKIP-DATE] Invoice date {inv_date_str} before cutoff "
                            f"{_cutoff}, not importing: {file_path.name}"
                        )
                        return False
                except ValueError:
                    logger.warning(
                        f"[SKIP-DATE-INVALID] Unparseable invoice_date '{inv_date_str}' "
                        f"in {file_path.name} — not importing"
                    )
                    return False

                ok = add_order(invoice_data)
                if ok:
                    logger.info(f"[ADDED] {invoice_data['customer_name']} — {file_path.name}")
                else:
                    logger.warning(f"[DUPLICATE] {file_path.name}")
                return ok
        except Exception:
            logger.error(f"Error processing {file_path.name}", exc_info=True)
            return False

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        logger.info("=" * 60)
        logger.info(f"File Watcher started — folder: {self.watch_folder}")
        logger.info(f"Poll every {self.poll_interval}s | "
                    f"{STABILITY_CHECKS} stability checks × {STABILITY_DELAY}s")
        logger.info("=" * 60)

        self._init_known_files()
        self.running = True

        try:
            while self.running:
                current = self.scan_folder()
                self.last_scan_time = time.strftime("%Y-%m-%d %H:%M:%S")

                for fp in current:
                    if fp.name not in self.known_files:
                        # mtime pre-filter: silently skip old files without opening them.
                        try:
                            if fp.stat().st_mtime < self._cutoff_ts:
                                self.known_files.add(fp.name)
                                continue
                        except OSError:
                            pass
                        # Filename-type filter: skip excluded file types.
                        if any(re.search(p, fp.name, re.IGNORECASE) for p in self._skip_patterns):
                            logger.info(f"[SKIP-TYPE] {fp.name}")
                            self.known_files.add(fp.name)
                            continue
                        logger.info(f"[NEW] {fp.name}")
                        if self.is_file_stable(fp):
                            self._process_file(fp)
                            self.known_files.add(fp.name)
                        else:
                            logger.info(f"[WAIT] {fp.name} not ready yet")

                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logger.info("File watcher stopped (KeyboardInterrupt)")
        except Exception:
            logger.error("Fatal watcher error", exc_info=True)
        finally:
            self.running = False


# ── Public start / stop helpers (called from main.py lifecycle hooks) ─────────

_watcher_thread:  Optional[threading.Thread] = None
_watcher_service: Optional[FileWatcher]      = None


def start_watcher() -> Optional[FileWatcher]:
    """
    Start the FileWatcher in a daemon thread if ENABLE_FILE_WATCHER is True.
    Returns the FileWatcher instance (or None if disabled).
    """
    global _watcher_thread, _watcher_service

    if not settings.ENABLE_FILE_WATCHER:
        logger.info("File watcher disabled (ENABLE_FILE_WATCHER=false).")
        return None

    try:
        _watcher_service = FileWatcher(
            watch_folder=settings.INVOICE_INPUT_FOLDER,
            poll_interval=POLL_INTERVAL,
        )
        _watcher_thread = threading.Thread(
            target=_watcher_service.run, daemon=True, name="file-watcher"
        )
        _watcher_thread.start()
        logger.info(f"File watcher thread started for: {settings.INVOICE_INPUT_FOLDER}")
        return _watcher_service
    except Exception:
        logger.error("Failed to start file watcher", exc_info=True)
        return None


def stop_watcher() -> None:
    """Signal the watcher to stop (called on application shutdown)."""
    global _watcher_service
    if _watcher_service:
        logger.info("Stopping file watcher…")
        _watcher_service.running = False

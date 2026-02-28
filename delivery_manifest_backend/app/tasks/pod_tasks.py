"""
app/tasks/pod_tasks.py

Background tasks for Proof-of-Delivery (POD) integration and the file watcher.

The FileWatcher polls the invoice input folder for new PDFs, checks that each
file has finished writing (size-stable), extracts invoice data, and saves it
to the database.  It runs in a daemon thread started at application startup.

Usage (called from app/main.py)::

    from app.tasks.pod_tasks import start_watcher, stop_watcher
"""

import logging
import os
import threading
import time
from pathlib import Path
from typing import Dict, Optional, Set

from app.core.config import settings
from app.core.logger import get_logger

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
        """Seed known_files from the current folder so we only process NEW files."""
        from app.services.manifest_service import get_all_orders
        from app.db.database import init_db

        init_db()
        all_orders = get_all_orders(allocated=True) + get_all_orders(allocated=False)
        processed  = {o.get("filename") for o in all_orders}

        for fp in self.scan_folder():
            self.known_files.add(fp.name)

        logger.info(
            f"Watcher initialised: {len(self.known_files)} folder PDFs, "
            f"{len(processed)} already in DB."
        )

    # ── Process a new file ────────────────────────────────────────────────────

    def _process_file(self, file_path: Path) -> bool:
        """Extract invoice data and save it to the database."""
        try:
            import invoice_processor  # type: ignore  (lives in the legacy root)

            invoice_data = invoice_processor.extract_invoice_data(str(file_path))
            if not invoice_data:
                logger.error(f"[SKIP] No data extracted from {file_path.name}")
                return False

            from app.services.manifest_service import add_order

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

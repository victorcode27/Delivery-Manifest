"""
app/core/logger.py

Centralised logging configuration.

Import `get_logger` (or the module-level `logger`) anywhere in the app
instead of calling `logging.getLogger(__name__)` directly, so the format
and handlers stay consistent across every module.

Usage::

    from app.core.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Server started")
"""

import logging
import os
from logging.handlers import RotatingFileHandler

from delivery_manifest_backend.app.core.config import settings


# ── Constants ─────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Log file lives in the project root (next to main.py)
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
LOG_FILE = os.path.join(LOG_DIR, "server.log")

# Max 5 MB per file, keep 3 backups
MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 3


def _configure_root_logger() -> None:
    """Configure the root logger once.  Called at import time."""
    os.makedirs(LOG_DIR, exist_ok=True)

    root = logging.getLogger()
    if root.handlers:
        # Already configured (e.g., pytest reloads the module)
        return

    root.setLevel(logging.DEBUG if settings.DEV_MODE else logging.INFO)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # Rotating file handler
    fh = RotatingFileHandler(
        LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
    )
    fh.setFormatter(formatter)
    root.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    root.addHandler(ch)


_configure_root_logger()


def get_logger(name: str) -> logging.Logger:
    """Return a named logger that inherits root configuration."""
    return logging.getLogger(name)


# Module-level default logger (used in this package's own log calls)
logger = get_logger("app")

"""
app/utils/file_utils.py

File upload and processing helpers.

These utilities are used by routes that handle multipart file uploads
(manifest Excel files, future bulk PDF imports, etc.).
"""

import os
import shutil
import uuid
from pathlib import Path
from typing import Optional

from fastapi import UploadFile

from delivery_manifest_backend.app.core.logger import get_logger

logger = get_logger(__name__)

# Maximum upload size: 50 MB
MAX_UPLOAD_BYTES = 50 * 1024 * 1024


def save_upload(
    file: UploadFile,
    destination_dir: str,
    rename_to: Optional[str] = None,
) -> str:
    """
    Save an uploaded file to *destination_dir*.

    Parameters
    ----------
    file            : FastAPI UploadFile object.
    destination_dir : Target directory (created if missing).
    rename_to       : Optional new filename (keeps original name if None).

    Returns
    -------
    str : Absolute path of the saved file.

    Raises
    ------
    ValueError : If *destination_dir* is outside the project root
                 (basic path-traversal guard).
    IOError    : If the file exceeds MAX_UPLOAD_BYTES.
    """
    dest_path = Path(destination_dir).resolve()
    dest_path.mkdir(parents=True, exist_ok=True)

    filename  = rename_to or file.filename or f"upload_{uuid.uuid4().hex}"
    file_path = dest_path / filename

    # Size guard
    file.file.seek(0, 2)           # seek to end
    size = file.file.tell()
    file.file.seek(0)              # rewind
    if size > MAX_UPLOAD_BYTES:
        raise IOError(
            f"Upload rejected: {filename} is {size} bytes "
            f"(max {MAX_UPLOAD_BYTES} bytes)."
        )

    with open(file_path, "wb") as buf:
        shutil.copyfileobj(file.file, buf)

    logger.info(f"Saved upload → {file_path} ({size} bytes)")
    return str(file_path)


def safe_filename(name: str) -> str:
    """
    Strip path separators and other dangerous characters from a filename.

    >>> safe_filename("../../etc/passwd")
    'etcpasswd'
    """
    return "".join(c for c in Path(name).name if c not in r'\/:*?"<>|')


def list_folder_files(folder: str, extension: str = "") -> list[str]:
    """
    Return filenames in *folder*, optionally filtered by *extension*.

    Example::

        pdfs = list_folder_files("/data/invoices", extension=".pdf")
    """
    path = Path(folder)
    if not path.is_dir():
        logger.warning(f"list_folder_files: '{folder}' is not a directory.")
        return []
    files = [
        f.name for f in path.iterdir()
        if f.is_file() and (not extension or f.suffix.lower() == extension.lower())
    ]
    return sorted(files)


def ensure_dir(path: str) -> str:
    """Create *path* and all parents if they do not exist.  Returns the path."""
    os.makedirs(path, exist_ok=True)
    return path

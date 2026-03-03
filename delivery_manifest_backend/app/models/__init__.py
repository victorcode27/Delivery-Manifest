"""
app/models/__init__.py

Import all ORM models here so that SQLAlchemy's mapper registry has every class
registered before configure_mappers() runs.

Importing this package (or anything that imports from it) guarantees that all
string-based relationship references (e.g. relationship("Manifest")) can be
resolved, even if only one model file was explicitly imported elsewhere.
"""

from .user import User
from .manifest import Manifest

__all__ = ["User", "Manifest"]

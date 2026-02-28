"""
app/core/config.py

Centralised settings loaded from environment variables / .env file.
All other modules should import `settings` from here — never call
os.getenv() directly in route or service code.
"""

import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Database ──────────────────────────────────────────────────────────────
    DATABASE_URL: str

    # ── Application behaviour ─────────────────────────────────────────────────
    DEV_MODE: bool = True
    ENV_MODE: str = "LAN"               # "LAN" | "PRODUCTION"

    # ── CORS ──────────────────────────────────────────────────────────────────
    # Comma-separated list of allowed origins.  "*" = allow all (LAN/dev only).
    ALLOWED_ORIGINS: str = "*"

    # ── File paths ────────────────────────────────────────────────────────────
    BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))
    MANIFEST_FOLDER: str = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "Manifests_Output"
    )
    # Absolute path to the frontend build output served as static files.
    # Defaults to a `static/` folder inside the backend root when left empty.
    STATIC_DIR: str = ""

    # ── File watcher ──────────────────────────────────────────────────────────
    ENABLE_FILE_WATCHER: bool = False
    INVOICE_INPUT_FOLDER: str = r"\\BRD-DESKTOP-ELV\storage"

    # ── Server ────────────────────────────────────────────────────────────────
    PORT: int = 8000
    HOST: str = "0.0.0.0"

    # ── JWT ───────────────────────────────────────────────────────────────────
    SECRET_KEY: str = "change-me-in-production"
    ALGORITHM: str = "HS256"                  # signing algorithm for JWT tokens
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"            # silently drop unknown env vars

    # Derived helpers ──────────────────────────────────────────────────────────
    @property
    def cors_origins(self) -> list[str]:
        """Return ALLOWED_ORIGINS as a Python list."""
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",")]


@lru_cache
def get_settings() -> Settings:
    """
    Return a cached singleton Settings instance.

    Usage anywhere in the app:
        from app.core.config import get_settings
        settings = get_settings()
    """
    return Settings()


# Convenience alias — import this directly in FastAPI `Depends()` calls
settings = get_settings()

"""
app/main.py

FastAPI application factory.

Start the server:
    uvicorn app.main:app --reload
    python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

import os
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.core.config import settings
from app.core.logger import get_logger
from app.db.database import init_db
from app.routes import auth, manifests, users
from app.tasks.pod_tasks import start_watcher, stop_watcher

logger = get_logger(__name__)

# ── Application factory ───────────────────────────────────────────────────────

app = FastAPI(
    title="Delivery Manifest API",
    version="3.0",
    description="Modular FastAPI backend for the Delivery Manifest System.",
)

# ── Request logging ───────────────────────────────────────────────────────────

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start    = time.perf_counter()
    response = await call_next(request)
    elapsed  = (time.perf_counter() - start) * 1000
    logger.info(f"{request.method} {request.url.path} → {response.status_code} ({elapsed:.1f}ms)")
    return response


# ── CORS ──────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────

app.include_router(auth.router,      prefix="/api")
app.include_router(users.router,     prefix="/api")
app.include_router(manifests.router, prefix="/api")

# ── Lifecycle events ──────────────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup() -> None:
    logger.info("Starting Delivery Manifest API…")
    init_db()
    watcher = start_watcher()
    app.state.watcher_service = watcher   # exposed via /watcher/status


@app.on_event("shutdown")
async def on_shutdown() -> None:
    stop_watcher()
    logger.info("Delivery Manifest API shut down.")


# ── Static frontend (mounted last; all API routes under /api take priority) ───

# Resolve to the backend root (delivery_manifest_backend/) — two levels up from
# this file.  Never serve the parent directory; that would expose source code,
# .env files, and database dumps to anonymous HTTP requests.
_BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_STATIC_DIR   = settings.STATIC_DIR or os.path.join(_BACKEND_ROOT, "static")

if os.path.isdir(_STATIC_DIR):
    app.mount("/", StaticFiles(directory=_STATIC_DIR, html=True), name="static")
else:
    logger.warning(
        f"Static directory '{_STATIC_DIR}' not found — frontend will not be served. "
        "Set STATIC_DIR in .env to the absolute path of your frontend build output."
    )


# ── Dev entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEV_MODE,
    )

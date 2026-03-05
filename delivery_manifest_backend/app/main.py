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
from fastapi.responses import FileResponse

from delivery_manifest_backend.app.core.config import settings
from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.db.database import init_db
import delivery_manifest_backend.app.models  # noqa: F401 – registers all ORM classes before configure_mappers()
from delivery_manifest_backend.app.routes import auth, manifests, users
from delivery_manifest_backend.app.tasks.pod_tasks import start_watcher, stop_watcher
from delivery_manifest_backend.app.tasks.cleanup_tasks import start_cleanup, stop_cleanup

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
    start_cleanup()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    stop_watcher()
    stop_cleanup()
    logger.info("Delivery Manifest API shut down.")


# ── Static frontend ───────────────────────────────────────────────────────────
# GET-only catch-all so POST/PUT/DELETE to /api/* are never intercepted.
#
# Path resolution (three dirname hops from this file):
#   delivery_manifest_backend/app/main.py → app/ → delivery_manifest_backend/ → repo root
_REPO_ROOT  = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_STATIC_DIR = settings.STATIC_DIR or _REPO_ROOT


@app.get("/{full_path:path}")
async def serve_frontend(full_path: str):
    """Serve static frontend files; fall back to index.html for SPA routing."""
    candidate = os.path.join(_STATIC_DIR, full_path)
    if full_path and os.path.isfile(candidate):
        return FileResponse(candidate)
    index = os.path.join(_STATIC_DIR, "index.html")
    if os.path.isfile(index):
        return FileResponse(index)
    logger.warning(f"Frontend not found: _STATIC_DIR={_STATIC_DIR!r}")
    from fastapi import HTTPException
    raise HTTPException(status_code=404, detail="Frontend not found")


# ── Dev entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "delivery_manifest_backend.app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEV_MODE,
    )

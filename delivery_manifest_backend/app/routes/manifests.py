"""
app/routes/manifests.py

All manifest-domain endpoints:
  /invoices          – invoice CRUD & search
  /manifests         – manifest details & file upload
  /manifest/current  – staging workflow
  /reports           – dispatch reports
  /areas, /customers – lookup helpers
  /settings          – app settings (drivers, routes, …)
  /trucks            – fleet management
  /customer-routes   – customer → route mappings
  /watcher/status    – file watcher probe
  /health            – server health check

Auth policy
-----------
  Public:
    GET /health, /watcher/status

  Any authenticated user (get_current_user):
    GET /invoices, /areas, /customers, /invoices/search,
        /manifests/search/query, /manifests/{n}, /reports, /reports/dispatched,
        /reports/outstanding, /settings/{cat}, /trucks, /customer-routes

  ADMIN or DISPATCH (require_dispatch_or_admin):
    POST /invoices/allocate, /invoices/refresh, /invoices/manual,
         /invoices/restore, /manifest/remove, /manifests/save, /reports
    GET  /manifest/current

  Admin only (require_admin):
    POST/PUT/DELETE /settings, /trucks, /customer-routes
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile

from delivery_manifest_backend.app.core.config import settings
from delivery_manifest_backend.app.core.deps import get_current_user, require_admin, require_dispatch_or_admin, require_office_read
from delivery_manifest_backend.app.core.logger import get_logger
from delivery_manifest_backend.app.schemas.manifest import (
    AllocateRequest,
    CustomerRouteRequest,
    ManualInvoiceRequest,
    ReportRequest,
    SettingRequest,
    SettingUpdateRequest,
    TruckRequest,
)
from delivery_manifest_backend.app.services import manifest_service

router = APIRouter(tags=["manifests"])
logger = get_logger(__name__)


# ── Input validation helper ───────────────────────────────────────────────────

def _validate_date(date_str: str, field: str) -> None:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format for {field}. Use YYYY-MM-DD.",
        )


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════════════════════════════════════

_server_start = datetime.now().isoformat()


@router.get("/health")
def health_check():
    return {
        "status":    "ok",
        "timestamp": _server_start,
        "dev_mode":  settings.DEV_MODE,
    }


# ══════════════════════════════════════════════════════════════════════════════
# INVOICES
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/invoices")
def get_invoices(
    area:   Optional[str] = None,
    limit:  int           = 2000,
    offset: int           = 0,
    current_user: dict = Depends(require_office_read),
):
    """
    Return pending invoices not in any staging session.

    Supports pagination via ``limit`` / ``offset``.  The response includes
    ``total`` (unfiltered row count) so the client can render page controls.
    Default page size of 200 is safe up to 100k rows.
    """
    try:
        orders, total = manifest_service.get_available_orders_excluding_staging(
            area=area, limit=limit, offset=offset
        )
        formatted = [
            {
                "filename":        o.get("filename"),
                "date_processed":  o.get("date_processed"),
                "customer_name":   o.get("customer_name"),
                "total_value":     o.get("total_value"),
                "order_number":    o.get("order_number"),
                "invoice_number":  o.get("invoice_number", "N/A"),
                "customer_number": o.get("customer_number", "N/A"),
                "invoice_date":    o.get("invoice_date", "N/A"),
                "area":            o.get("area", "UNKNOWN"),
            }
            for o in orders
        ]
        logger.info(f"Fetched {len(formatted)} invoices (offset={offset}, total={total})")
        return {
            "invoices": formatted,
            "count":    len(formatted),
            "total":    total,
            "limit":    limit,
            "offset":   offset,
        }
    except Exception:
        logger.error("Error fetching invoices", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/areas")
def get_areas(current_user: dict = Depends(get_current_user)):
    try:
        return {"areas": sorted(manifest_service.get_areas())}
    except Exception:
        logger.error("Error fetching areas", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/customers")
def get_customers(current_user: dict = Depends(get_current_user)):
    try:
        return {"customers": manifest_service.get_all_customers()}
    except Exception:
        logger.error("Error fetching customers", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/invoices/allocate")
def allocate_invoices(
    request_data: AllocateRequest,
    current_user: dict = Depends(require_dispatch_or_admin),
):
    """Stage invoices for a manifest (does not finalise until report is saved)."""
    try:
        username = current_user["username"]
        added = manifest_service.add_to_staging(username, request_data.filenames)
        if added > 0:
            logger.info(f"Staged {added} invoices for '{username}'")
            return {"message": f"Added {added} invoices to manifest", "added": added}
        return {"message": "Invoices already in manifest or not found", "added": 0}
    except Exception:
        logger.error("Error staging invoices", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/invoices/refresh")
def refresh_invoices(current_user: dict = Depends(require_dispatch_or_admin)):
    """Trigger a re-scan of the invoice input folder."""
    try:
        manifest_service.refresh_invoices()
        return {"message": "Invoice scan completed"}
    except Exception:
        logger.error("Error refreshing invoices", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/invoices/manual")
def add_manual_invoice(
    req: ManualInvoiceRequest,
    current_user: dict = Depends(require_dispatch_or_admin),
):
    """Manually add an invoice that has no PDF source."""
    try:
        filename = manifest_service.create_manual_invoice(
            customer_name=req.customer_name,
            total_value=req.total_value,
            invoice_number=req.invoice_number,
            order_number=req.order_number,
            customer_number=req.customer_number,
            area=req.area,
        )
        if filename is None:
            raise HTTPException(status_code=400, detail="Failed to add invoice (duplicate?)")
        logger.info(f"Added manual invoice: {req.invoice_number}")
        return {"message": "Invoice added successfully", "filename": filename}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error adding manual invoice", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/invoices/search")
def search_invoices(q: str, current_user: dict = Depends(require_office_read)):
    try:
        return {"results": manifest_service.search_orders(q)}
    except Exception:
        logger.error("Error searching invoices", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/invoices/restore")
def restore_invoices(
    request: AllocateRequest,
    current_user: dict = Depends(require_dispatch_or_admin),
):
    """De-allocate invoices back to pending status."""
    try:
        updated = manifest_service.deallocate_orders(request.filenames)
        if updated > 0:
            return {"message": f"Restored {updated} invoices", "count": updated}
        raise HTTPException(status_code=404, detail="No invoices found to restore")
    except HTTPException:
        raise
    except Exception:
        logger.error("Error restoring invoices", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ══════════════════════════════════════════════════════════════════════════════
# MANIFEST STAGING
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/manifest/current")
def get_current_manifest_staging(
    current_user: dict = Depends(require_dispatch_or_admin),
    manifest_number: Optional[str] = None,
):
    try:
        username = current_user["username"]
        invoices = manifest_service.get_current_manifest(username, manifest_number)
        return {"invoices": invoices, "count": len(invoices)}
    except Exception:
        logger.error("Error fetching current manifest", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/manifest/remove")
def remove_from_manifest_staging(
    request_data: AllocateRequest,
    current_user: dict = Depends(require_dispatch_or_admin),
):
    try:
        username = current_user["username"]
        removed  = manifest_service.remove_from_staging(username, request_data.filenames)
        return {
            "message": f"Removed {removed} invoices from manifest" if removed else
                       "No invoices found in manifest to remove",
            "removed": removed,
        }
    except Exception:
        logger.error("Error removing from staging", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ══════════════════════════════════════════════════════════════════════════════
# MANIFESTS (detail & file upload)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/manifests/search/query")
def search_manifests(q: str, current_user: dict = Depends(require_office_read)):
    try:
        details = manifest_service.get_manifest_details(q)
        return {"match": True, "manifest": details} if details else {"match": False}
    except Exception:
        logger.error("Error searching manifest", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/manifests/{manifest_number}")
def get_manifest_details(manifest_number: str, current_user: dict = Depends(require_office_read)):
    try:
        details = manifest_service.get_manifest_details(manifest_number)
        if not details:
            raise HTTPException(status_code=404, detail="Manifest not found")
        return details
    except HTTPException:
        raise
    except Exception:
        logger.error("Error fetching manifest", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/manifests/save")
async def save_manifest_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(require_dispatch_or_admin),
):
    """Save the generated manifest Excel file to disk."""
    try:
        content = await file.read()
        path = manifest_service.save_manifest_file(
            content, file.filename, settings.MANIFEST_FOLDER
        )
        logger.info(f"Saved manifest to {path}")
        return {"message": "Manifest saved", "path": path}
    except Exception:
        logger.error("Error saving manifest file", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ══════════════════════════════════════════════════════════════════════════════
# REPORTS
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/reports")
def save_report(
    request_data: ReportRequest,
    current_user: dict = Depends(require_dispatch_or_admin),
):
    try:
        report_dict               = request_data.dict()
        report_dict["session_id"] = current_user["username"]
        report_id                 = manifest_service.save_report(report_dict)
        logger.info(f"Saved report {request_data.manifestNumber} (id={report_id})")
        return {"message": "Report saved", "id": report_id}
    except Exception:
        logger.error("Error saving report", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/reports")
def get_reports(date_from: Optional[str] = None, date_to: Optional[str] = None, current_user: dict = Depends(require_office_read)):
    """Legacy endpoint — prefer /reports/dispatched."""
    try:
        reports = manifest_service.get_reports(date_from, date_to)
        return {"reports": reports, "count": len(reports)}
    except Exception:
        logger.error("Error fetching reports", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/reports/dispatched")
def get_dispatched_invoices(
    date_from:   Optional[str] = None,
    date_to:     Optional[str] = None,
    filter_type: str           = "dispatch",
    search:      Optional[str] = None,
    route:       Optional[str] = None,
    limit:       int           = 50,
    offset:      int           = 0,
    sort_by:     str           = "date_dispatched",
    sort_order:  str           = "DESC",
    current_user: dict = Depends(require_office_read),
):
    """Paginated invoice-level dispatch history."""
    try:
        if date_from:
            _validate_date(date_from, "date_from")
        if date_to:
            _validate_date(date_to, "date_to")
        if filter_type not in ("dispatch", "manifest"):
            filter_type = "dispatch"

        results, total = manifest_service.get_dispatched_invoices(
            date_from=date_from,
            date_to=date_to,
            filter_type=filter_type,
            search_query=search,
            route=route,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        return {
            "invoices":    results,
            "total":       total,
            "page":        offset // limit if limit > 0 else 0,
            "limit":       limit,
            "filter_type": filter_type,
        }
    except HTTPException:
        raise
    except Exception:
        logger.error("Error fetching dispatched invoices", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/reports/outstanding")
def get_outstanding_invoices(
    limit:  int = 500,
    offset: int = 0,
    current_user: dict = Depends(require_office_read),
):
    """
    Return invoices not yet included in any dispatch report.

    Paginated via ``limit`` / ``offset``.  Default cap of 500 is generous
    enough for day-to-day use while preventing unbounded scans.
    """
    try:
        results, total = manifest_service.get_outstanding_orders(limit=limit, offset=offset)
        return {
            "orders": results,
            "count":  len(results),
            "total":  total,
            "limit":  limit,
            "offset": offset,
        }
    except Exception:
        logger.error("Error fetching outstanding orders", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ══════════════════════════════════════════════════════════════════════════════
# SETTINGS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/settings/{category}")
def get_settings(category: str, current_user: dict = Depends(get_current_user)):
    try:
        return {"category": category, "values": manifest_service.get_settings(category)}
    except Exception:
        logger.error("Error fetching settings", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/settings")
def add_setting(
    request: SettingRequest,
    current_user: dict = Depends(require_admin),
):
    try:
        ok = manifest_service.add_setting(request.category, request.value)
        if not ok:
            raise HTTPException(status_code=400, detail="Value already exists")
        return {"message": f"Added '{request.value}' to {request.category}"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error adding setting", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/settings")
def update_setting(
    request: SettingUpdateRequest,
    current_user: dict = Depends(require_admin),
):
    try:
        ok = manifest_service.update_setting(request.category, request.old_value, request.new_value)
        if not ok:
            raise HTTPException(status_code=404, detail="Setting not found")
        return {"message": f"Updated '{request.old_value}' to '{request.new_value}'"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error updating setting", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/settings/{category}/{value}")
def delete_setting(
    category: str,
    value: str,
    current_user: dict = Depends(require_admin),
):
    try:
        ok = manifest_service.delete_setting(category, value)
        if not ok:
            raise HTTPException(status_code=404, detail="Setting not found")
        return {"message": f"Deleted '{value}' from {category}"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error deleting setting", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ══════════════════════════════════════════════════════════════════════════════
# TRUCKS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/trucks")
def get_trucks(current_user: dict = Depends(get_current_user)):
    try:
        return {"trucks": manifest_service.get_trucks()}
    except Exception:
        logger.error("Error fetching trucks", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/trucks")
def add_truck(
    request: TruckRequest,
    current_user: dict = Depends(require_admin),
):
    try:
        ok = manifest_service.add_truck(request.reg, request.driver, request.assistant, request.checker)
        if not ok:
            raise HTTPException(status_code=400, detail="Truck registration already exists")
        return {"message": f"Added truck '{request.reg}'"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error adding truck", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/trucks/{reg}")
def update_truck(
    reg: str,
    request: TruckRequest,
    current_user: dict = Depends(require_admin),
):
    try:
        ok = manifest_service.update_truck(reg, request.driver, request.assistant, request.checker)
        if not ok:
            raise HTTPException(status_code=404, detail="Truck not found")
        return {"message": f"Updated truck '{reg}'"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error updating truck", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/trucks/{reg}")
def delete_truck(
    reg: str,
    current_user: dict = Depends(require_admin),
):
    try:
        ok = manifest_service.delete_truck(reg)
        if not ok:
            raise HTTPException(status_code=404, detail="Truck not found")
        return {"message": f"Deleted truck '{reg}'"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error deleting truck", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ══════════════════════════════════════════════════════════════════════════════
# CUSTOMER ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/customer-routes")
def get_customer_routes(current_user: dict = Depends(get_current_user)):
    try:
        return {"routes": manifest_service.get_customer_routes()}
    except Exception:
        logger.error("Error fetching customer routes", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/customer-routes")
def add_customer_route(
    request: CustomerRouteRequest,
    current_user: dict = Depends(require_admin),
):
    try:
        ok = manifest_service.add_customer_route(request.customer_name, request.route_name)
        if not ok:
            raise HTTPException(status_code=400, detail="Failed to save mapping")
        return {"message": f"Assigned '{request.customer_name}' to '{request.route_name}'"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error adding customer route", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/customer-routes/{customer_name}")
def delete_customer_route(
    customer_name: str,
    current_user: dict = Depends(require_admin),
):
    try:
        ok = manifest_service.delete_customer_route(customer_name)
        if not ok:
            raise HTTPException(status_code=404, detail="Mapping not found")
        return {"message": f"Deleted route for '{customer_name}'"}
    except HTTPException:
        raise
    except Exception:
        logger.error("Error deleting customer route", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ══════════════════════════════════════════════════════════════════════════════
# FILE WATCHER STATUS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/watcher/status")
def get_watcher_status(request: Request):
    """Check whether the file watcher background service is running."""
    # The watcher instance is stored on the FastAPI app state by main.py
    watcher = getattr(request.app.state, "watcher_service", None)
    if watcher and watcher.running:
        return {
            "status":        "running",
            "folder":        str(watcher.watch_folder),
            "poll_interval": watcher.poll_interval,
            "last_scan":     getattr(watcher, "last_scan_time", "Unknown"),
        }
    return {"status": "stopped"}

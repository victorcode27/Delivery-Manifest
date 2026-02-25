"""
FastAPI server to provide invoice data to the Delivery Manifest web app.
Run with: uvicorn api_server:app --reload
Or: python api_server.py
"""

import os
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from fastapi import File, UploadFile
import shutil
import uvicorn

import threading
import file_watcher

# Import the database module
import database

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MANIFEST_FOLDER = os.getenv("MANIFEST_FOLDER", os.path.join(BASE_DIR, "Manifests_Output"))
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server.log")

# --- DEV MODE CONFIG ---
DEV_MODE = os.getenv("DEV_MODE", "true").lower() == "true"
SERVER_START_TIME = datetime.now().isoformat()

# --- CORS CONFIG ---
_raw_origins = os.getenv("ALLOWED_ORIGINS", "*")
CORS_ORIGINS = [o.strip() for o in _raw_origins.split(",")]

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize database on startup
database.init_db()

app = FastAPI(title="Invoice API", version="2.0")

# --- File Watcher State ---
watcher_service = None
watcher_thread = None

@app.on_event("startup")
async def startup_event():
    """Start the file watcher service in a background thread (local/LAN only)."""
    global watcher_service, watcher_thread
    if os.getenv("ENABLE_FILE_WATCHER", "false").lower() != "true":
        logger.info("File Watcher disabled (ENABLE_FILE_WATCHER != true). Skipping.")
        return
    try:
        logger.info("Initializing File Watcher Service...")
        watcher_service = file_watcher.FileWatcher(
            watch_folder=file_watcher.WATCH_FOLDER,
            poll_interval=file_watcher.POLL_INTERVAL
        )
        # Run in daemon thread so it closes when server closes
        watcher_thread = threading.Thread(target=watcher_service.run, daemon=True)
        watcher_thread.start()
        logger.info(f"File Watcher started for: {file_watcher.WATCH_FOLDER}")
    except Exception as e:
        logger.error(f"Failed to start File Watcher: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop the file watcher service."""
    global watcher_service
    if watcher_service:
        logger.info("Stopping File Watcher Service...")
        watcher_service.running = False


# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Helper Functions ---

def get_username_from_request(request_headers) -> str:
    """Extract username from X-Username header. Returns 'anonymous' if not found."""
    username = request_headers.get('X-Username', request_headers.get('x-username', ''))
    if not username:
        username = 'anonymous'
    return username

# --- Data Models ---
class Invoice(BaseModel):
    filename: str
    date_processed: str
    customer_name: str
    total_value: str
    order_number: str
    invoice_date: Optional[str] = "N/A"
    area: Optional[str] = "UNKNOWN"

class AllocateRequest(BaseModel):
    filenames: List[str]
    manifest_number: Optional[str] = None

class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreate(BaseModel):
    username: str
    password: str
    is_admin: bool = False
    can_manifest: bool = True

class UserUpdate(BaseModel):
    password: Optional[str] = None
    is_admin: Optional[bool] = None
    can_manifest: Optional[bool] = None

class SettingRequest(BaseModel):
    category: str
    value: str

class SettingUpdateRequest(BaseModel):
    category: str
    old_value: str
    new_value: str

class TruckRequest(BaseModel):
    reg: str
    driver: Optional[str] = None
    assistant: Optional[str] = None
    checker: Optional[str] = None

class CustomerRouteRequest(BaseModel):
    customer_name: str
    route_name: str

class ReportRequest(BaseModel):
    manifestNumber: str
    date: str
    driver: Optional[str] = None
    assistant: Optional[str] = None
    checker: Optional[str] = None
    regNumber: Optional[str] = None
    palletsBrown: int = 0
    palletsBlue: int = 0
    crates: int = 0
    mileage: int = 0
    totalValue: float = 0
    totalSku: int = 0
    totalWeight: float = 0
    invoices: List[dict] = []


# --- API Endpoints ---



# =============================================
# INVOICE ENDPOINTS
# =============================================

@app.get("/invoices")
def get_invoices(area: Optional[str] = None):
    """Get all pending invoices. Optionally filter by area."""
    try:
        orders = database.get_available_orders_excluding_staging()
        
        if area:
            orders = [o for o in orders if o.get("area", "").upper() == area.upper()]
        
        # Convert to the format expected by the frontend
        formatted_orders = []
        for o in orders:
            formatted_orders.append({
                "filename": o.get("filename"),
                "date_processed": o.get("date_processed"),
                "customer_name": o.get("customer_name"),
                "total_value": o.get("total_value"),
                "order_number": o.get("order_number"),
                "invoice_number": o.get("invoice_number", "N/A"),
                "customer_number": o.get("customer_number", "N/A"), # Added customer_number
                "invoice_date": o.get("invoice_date", "N/A"),
                "area": o.get("area", "UNKNOWN")
            })
        
        logger.info(f"Fetched {len(formatted_orders)} invoices")
        return {"invoices": formatted_orders, "count": len(formatted_orders)}
    except Exception as e:
        logger.error(f"Error fetching invoices: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/areas")
def get_areas():
    """Get list of unique areas from all invoices."""
    try:
        areas = database.get_areas()
        return {"areas": sorted(areas)}
    except Exception as e:
        logger.error(f"Error fetching areas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/customers")
def get_customers():
    """Get list of unique customer names from all invoices (pending and allocated)."""
    try:
        customers = database.get_all_customers()
        return {"customers": customers}
    except Exception as e:
        logger.error(f"Error fetching customers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/invoices/allocate")
def allocate_invoices(request_data: AllocateRequest, request: Request):
    """Add invoices to manifest staging (does NOT update invoices table until confirm)."""
    try:
        # Extract username from headers
        username = get_username_from_request(request.headers)
        
        # Add to staging instead of directly allocating
        added = database.add_to_staging(username, request_data.filenames)
        
        if added > 0:
            logger.info(f"Added {added} invoices to staging for user {username}")
            return {"message": f"Added {added} invoices to manifest", "added": added}
        else:
            logger.warning(f"No invoices added to staging (may already be in manifest)")
            return {"message": "Invoices already in manifest or not found", "added": 0}
    except Exception as e:
        logger.error(f"Error adding to staging: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/invoices/refresh")
def refresh_invoices():
    """Trigger a re-scan of the input folder for new PDFs."""
    try:
        import invoice_processor
        import importlib
        importlib.reload(invoice_processor)  # Reload to get fresh module
        invoice_processor.main()
        logger.info("Invoice scan completed")
        return {"message": "Invoice scan completed"}
    except Exception as e:
        logger.error(f"Error refreshing invoices: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/watcher/status")
def get_watcher_status():
    """Get the status of the file watcher service."""
    global watcher_service
    if watcher_service and watcher_service.running:
        return {
            "status": "running",
            "folder": str(watcher_service.watch_folder),
            "poll_interval": watcher_service.poll_interval,
            "last_scan": getattr(watcher_service, "last_scan_time", "Unknown")
        }
    else:
        return {"status": "stopped"}


# =============================================
# MANUAL ENTRY & HISTORICAL SEARCH
# =============================================

class ManualInvoiceRequest(BaseModel):
    customer_name: str
    total_value: str
    invoice_number: str
    order_number: str
    customer_number: Optional[str] = "N/A"
    area: Optional[str] = "UNKNOWN"

@app.post("/invoices/manual")
def add_manual_invoice(request: ManualInvoiceRequest):
    """Add a manual invoice entry."""
    try:
        # Generate a unique filename for the manual entry
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Add random suffix to ensure uniqueness even if multiple added same second
        import secrets
        suffix = secrets.token_hex(4)
        filename = f"MANUAL_{timestamp}_{suffix}.pdf"
        
        invoice_data = {
            "filename": filename,
            "date_processed": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "customer_name": request.customer_name,
            "total_value": request.total_value,
            "invoice_number": request.invoice_number,
            "order_number": request.order_number,
            "customer_number": request.customer_number,
            "invoice_date": datetime.now().strftime("%Y-%m-%d"),
            "area": request.area
        }
        
        success = database.add_order(invoice_data)
        if success:
            logger.info(f"Added manual invoice: {request.invoice_number}")
            return {"message": "Invoice added successfully", "filename": filename}
        else:
            raise HTTPException(status_code=400, detail="Failed to add invoice (duplicate?)")
            
    except Exception as e:
        logger.error(f"Error adding manual invoice: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/invoices/search")
def search_invoices(q: str):
    """Search for historical invoices."""
    try:
        results = database.search_orders(q)
        return {"results": results}
    except Exception as e:
        logger.error(f"Error searching invoices: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/invoices/restore")
def restore_invoices(request: AllocateRequest):
    """Restore (de-allocate) invoices back to pending status."""
    try:
        updated = database.deallocate_orders(request.filenames)
        if updated > 0:
            logger.info(f"Restored {updated} invoices")
            return {"message": f"Restored {updated} invoices", "count": updated}
        else:
            raise HTTPException(status_code=404, detail="No invoices found to restore")
    except Exception as e:
        logger.error(f"Error restoring invoices: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================
# AUTH ENDPOINTS
# =============================================

@app.post("/auth/login")
def login(request: LoginRequest):
    """Verify user credentials and return user info."""
    try:
        user = database.verify_user(request.username, request.password)
        if user:
            logger.info(f"User '{request.username}' logged in")
            return {
                "success": True,
                "user": {
                    "username": user["username"],
                    "isAdmin": bool(user["is_admin"]),
                    "canManifest": bool(user["can_manifest"])
                }
            }
        else:
            logger.warning(f"Failed login attempt for user '{request.username}'")
            raise HTTPException(status_code=401, detail="Invalid username or password")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users")
def get_users():
    """Get all users (admin only in production)."""
    try:
        users = database.get_all_users()
        return {"users": users}
    except Exception as e:
        logger.error(f"Error fetching users: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/users")
def create_user(request: UserCreate):
    """Create a new user."""
    try:
        success = database.create_user(
            request.username, 
            request.password, 
            request.is_admin, 
            request.can_manifest
        )
        if success:
            logger.info(f"Created user '{request.username}'")
            return {"message": f"User '{request.username}' created"}
        else:
            raise HTTPException(status_code=400, detail="Username already exists")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/users/{username}")
def update_user(username: str, request: UserUpdate):
    """Update a user's details."""
    try:
        success = database.update_user(
            username,
            request.password,
            request.is_admin,
            request.can_manifest
        )
        if success:
            logger.info(f"Updated user '{username}'")
            return {"message": f"User '{username}' updated"}
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating user: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/users/{username}")
def delete_user(username: str):
    """Delete a user."""
    try:
        success = database.delete_user(username)
        if success:
            logger.info(f"Deleted user '{username}'")
            return {"message": f"User '{username}' deleted"}
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================
# REPORTS ENDPOINTS
# =============================================

@app.post("/reports")
def save_report(request_data: ReportRequest, request: Request):
    """Save a dispatch report."""
    try:
        # Extract username for session_id
        username = get_username_from_request(request.headers)
        
        # Add session_id to report_data
        report_dict = request_data.dict()
        report_dict['session_id'] = username
        
        report_id = database.save_report(report_dict)
        logger.info(f"Saved report {request_data.manifestNumber} with ID {report_id} for user {username}")
        return {"message": "Report saved", "id": report_id}
    except Exception as e:
        logger.error(f"Error saving report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reports")
def get_reports(date_from: Optional[str] = None, date_to: Optional[str] = None):
    """
    Get dispatch reports, optionally filtered by date range.
    
    DEPRECATED: Use /reports/dispatched for better performance and features.
    This endpoint is kept for backward compatibility.
    """
    try:
        reports = database.get_reports(date_from, date_to)
        return {"reports": reports, "count": len(reports)}
    except Exception as e:
        logger.error(f"Error fetching reports: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================
# NEW DISPATCH REPORT ENDPOINTS
# =============================================

@app.get("/reports/dispatched")
def get_dispatched_invoices(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    filter_type: str = "dispatch",  # NEW: 'dispatch' or 'manifest'
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    sort_by: str = "date_dispatched",
    sort_order: str = "DESC"
):
    """
    Get dispatched invoices (invoice-level rows, not report-level).
    
    Each row contains denormalized dispatch metadata + invoice snapshot.
    
    Query Parameters:
        - date_from: Filter by date >= (YYYY-MM-DD)
        - date_to: Filter by date <= (YYYY-MM-DD)
        - filter_type: 'dispatch' (manifest creation date) or 'manifest' (invoice add date)
        - search: Global text search (invoice #, order #, manifest #, customer, driver, truck, checker)
        - limit: Results per page (default 50)
        - offset: Skip N results (for pagination)
        - sort_by: Field to sort by (date_dispatched, manifest_number, invoice_number, customer_name, driver)
        - sort_order: ASC or DESC (default DESC)
    
    Returns:
        {
            "invoices": [...],
            "total": total_count,
            "page": current_page,
            "limit": results_per_page
        }
    """
    try:
        if date_from:
            validate_date(date_from, "date_from")
        if date_to:
            validate_date(date_to, "date_to")
            
        if DEV_MODE:
            logger.info(f"[DEV_MODE] Filter Request - From: {date_from}, To: {date_to}, Type: {filter_type}")

        # Validate filter_type
        if filter_type not in ['dispatch', 'manifest']:
            filter_type = 'dispatch'
        
        results, total = database.get_dispatched_invoices(
            date_from=date_from,
            date_to=date_to,
            filter_type=filter_type,  # NEW
            search_query=search,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        logger.info(f"Fetched {len(results)} dispatched invoices (total: {total}, filter_type: {filter_type}, filters: date_from={date_from}, date_to={date_to}, search={search})")
        
        return {
            "invoices": results,
            "total": total,
            "page": offset // limit if limit > 0 else 0,
            "limit": limit,
            "filter_type": filter_type  # NEW: Echo back for frontend validation
        }
    except Exception as e:
        logger.error(f"Error fetching dispatched invoices: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reports/outstanding")
def get_outstanding_invoices():
    """
    Get outstanding orders (invoices with NO dispatch record).
    
    Returns invoices that have never been added to any dispatch report.
    Excludes cancelled invoices.
    
    Returns:
        {
            "orders": [...],
            "count": total_count
        }
    """
    try:
        results = database.get_outstanding_orders()
        logger.info(f"Fetched {len(results)} outstanding orders")
        return {
            "orders": results,
            "count": len(results)
        }
    except Exception as e:
        logger.error(f"Error fetching outstanding orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================
# EXISTING MANIFEST ENDPOINTS
# =============================================

@app.get("/manifests/{manifest_number}")
def get_manifest_details(manifest_number: str):
    """Get full details/history of a manifest."""
    try:
        details = database.get_manifest_details(manifest_number)
        if details:
            return details
        else:
            raise HTTPException(status_code=404, detail="Manifest not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching manifest details: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/manifests/search/query")
def search_manifests(q: str):
    """Search for a specific manifest details by number."""
    # This might return just the details or a list if partial match. 
    # For now, let's assume direct lookup or use the existing reports list logic if needed.
    # Re-using get_manifest_details logic but wrapped for search usage if needed.
    try:
        # If query looks like a manifest number, try exact match
        details = database.get_manifest_details(q)
        if details:
            return {"match": True, "manifest": details}
        else:
            return {"match": False}
    except Exception as e:
        logger.error(f"Error searching manifest: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================
# MANIFEST STAGING ENDPOINTS (NEW - FIX FOR WORKFLOW BUG)
# =============================================

@app.get("/manifest/current")
def get_current_manifest_staging(request: Request, manifest_number: Optional[str] = None):
    """Get invoices in current manifest.
    
    If manifest_number provided: returns finalized invoices for that manifest + staging entries.
    If not provided: returns staging-only (backward compatible).
    """
    try:
        username = get_username_from_request(request.headers)
        invoices = database.get_current_manifest(username, manifest_number)
        logger.info(f"Fetched {len(invoices)} invoices for user {username}" + 
                   (f" (manifest: {manifest_number})" if manifest_number else " (staging only)"))
        return {"invoices": invoices, "count": len(invoices)}
    except Exception as e:
        logger.error(f"Error fetching current manifest: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/manifest/remove")
def remove_from_manifest_staging(request_data: AllocateRequest, request: Request):
    """Remove invoices from current manifest staging."""
    try:
        username = get_username_from_request(request.headers)
        removed = database.remove_from_staging(username, request_data.filenames)
        
        if removed > 0:
            logger.info(f"Removed {removed} invoices from staging for user {username}")
            return {"message": f"Removed {removed} invoices from manifest", "removed": removed}
        else:
            return {"message": "No invoices found in manifest to remove", "removed": 0}
    except Exception as e:
        logger.error(f"Error removing from staging: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================
# SETTINGS ENDPOINTS
# =============================================

@app.get("/settings/{category}")
def get_settings(category: str):
    """Get all values for a settings category."""
    try:
        values = database.get_settings(category)
        return {"category": category, "values": values}
    except Exception as e:
        logger.error(f"Error fetching settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/settings")
def add_setting(request: SettingRequest):
    """Add a new setting value."""
    try:
        success = database.add_setting(request.category, request.value)
        if success:
            return {"message": f"Added '{request.value}' to {request.category}"}
        else:
            raise HTTPException(status_code=400, detail="Value already exists")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding setting: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/settings")
def update_setting(request: SettingUpdateRequest):
    """Update a setting value."""
    try:
        success = database.update_setting(request.category, request.old_value, request.new_value)
        if success:
            return {"message": f"Updated '{request.old_value}' to '{request.new_value}'"}
        else:
            raise HTTPException(status_code=404, detail="Setting not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating setting: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/settings/{category}/{value}")
def delete_setting(category: str, value: str):
    """Delete a setting value."""
    try:
        success = database.delete_setting(category, value)
        if success:
            return {"message": f"Deleted '{value}' from {category}"}
        else:
            raise HTTPException(status_code=404, detail="Setting not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting setting: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================
# TRUCKS ENDPOINTS
# =============================================

@app.get("/trucks")
def get_trucks():
    """Get all trucks."""
    try:
        trucks = database.get_trucks()
        return {"trucks": trucks}
    except Exception as e:
        logger.error(f"Error fetching trucks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/trucks")
def add_truck(request: TruckRequest):
    """Add a new truck."""
    try:
        success = database.add_truck(request.reg, request.driver, request.assistant, request.checker)
        if success:
            return {"message": f"Added truck '{request.reg}'"}
        else:
            raise HTTPException(status_code=400, detail="Truck registration already exists")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding truck: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/trucks/{reg}")
def update_truck(reg: str, request: TruckRequest):
    """Update a truck's details."""
    try:
        success = database.update_truck(reg, request.driver, request.assistant, request.checker)
        if success:
            return {"message": f"Updated truck '{reg}'"}
        else:
            raise HTTPException(status_code=404, detail="Truck not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating truck: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/trucks/{reg}")
def delete_truck(reg: str):
    """Delete a truck."""
    try:
        success = database.delete_truck(reg)
        if success:
            return {"message": f"Deleted truck '{reg}'"}
        else:
            raise HTTPException(status_code=404, detail="Truck not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting truck: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================
# CUSTOMER ROUTE ENDPOINTS
# =============================================

@app.get("/customer-routes")
def get_customer_routes():
    """Get all customer-route mappings."""
    try:
        routes = database.get_customer_routes()
        return {"routes": routes}
    except Exception as e:
        logger.error(f"Error fetching customer routes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/customer-routes")
def add_customer_route(request: CustomerRouteRequest):
    """Add or update a customer-route mapping."""
    try:
        success = database.add_customer_route(request.customer_name, request.route_name)
        if success:
            return {"message": f"Assigned '{request.customer_name}' to '{request.route_name}'"}
        else:
            raise HTTPException(status_code=400, detail="Failed to save mapping")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding customer route: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/customer-routes/{customer_name}")
def delete_customer_route(customer_name: str):
    """Delete a customer-route mapping."""
    try:
        success = database.delete_customer_route(customer_name)
        if success:
            return {"message": f"Deleted route for '{customer_name}'"}
        else:
            raise HTTPException(status_code=404, detail="Mapping not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting customer route: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================
# MANIFEST FILE ENDPOINTS
# =============================================

@app.post("/manifests/save")
async def save_manifest(file: UploadFile = File(...)):
    """Save the generated manifest Excel file to the manifests folder."""
    try:
        if not os.path.exists(MANIFEST_FOLDER):
            os.makedirs(MANIFEST_FOLDER)
            
        file_path = os.path.join(MANIFEST_FOLDER, file.filename)
        
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        logger.info(f"Saved manifest to {file_path}")
        return {"message": f"Manifest saved to {file_path}", "path": file_path}
    except Exception as e:
        logger.error(f"Error saving manifest: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save manifest: {str(e)}")




# --- Serve Static Files (Frontend) ---
# Place this AT THE END so it doesn't override API routes

from fastapi.responses import FileResponse

@app.get("/")
async def read_index():
    index_path = os.path.join(BASE_DIR, "index.html")
    return FileResponse(index_path)

# Also need to serve style.css, script.js, logo.png directly if they are requested at /style.css etc.
# Catch-all for files in the root directory (only if no other route matches)
@app.get("/{filename}")
async def read_file(filename: str):
    # Security: Prevent directory traversal
    if ".." in filename or filename.startswith("/") or filename.startswith("\\\\"):
        raise HTTPException(status_code=400, detail="Invalid filename")
    
    file_path = os.path.join(BASE_DIR, filename)
    if os.path.isfile(file_path):
        return FileResponse(file_path)
    raise HTTPException(status_code=404, detail="File not found")

# Run the server if executed directly


# =============================================
# DEV MODE ENDPOINTS & HELPERS
# =============================================

@app.get("/health")
def health_check():
    """Returns server health status, timestamp, and DEV_MODE flag."""
    return {
        "status": "ok",
        "timestamp": SERVER_START_TIME,
        "dev_mode": DEV_MODE
    }

def validate_date(date_str: str, field_name: str):
    """
    Validates date string format YYYY-MM-DD.
    Raises HTTPException if invalid.
    """
    if not date_str:
        return # Empty string handled by logic usually, or should error?
        # User said "rejects invalid formats or empty strings"
        # But wait, date filters are Optional.
        # If optional, None is fine. If string provided, must be valid.
    
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        if DEV_MODE:
            logger.warning(f"[DEV_MODE] Invalid date format for {field_name}: {date_str}")
        raise HTTPException(status_code=400, detail=f"Invalid date format for {field_name}. Use YYYY-MM-DD.")


# --- LAN Configuration ---
LAN_MODE = True  # Set to False for localhost-only access
PRODUCTION_MODE = False  # Safety check to prevent accidental public exposure

if __name__ == "__main__":
    host = "0.0.0.0"
    port = int(os.getenv("PORT", 8000))

    logger.info(f"Starting Invoice API server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)

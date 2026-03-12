"""
Invoice Processor - Scans PDF invoices and saves to database.
Also handles file organization (moves processed/failed files).
"""

import os
import datetime
import re
import glob
import shutil
import logging

# Try to import pdfplumber, handle if missing
try:
    import pdfplumber
except ImportError:
    print("Error: pdfplumber library is not installed.")
    print("Please run: pip install pdfplumber")
    exit(1)

# Import database module
import database
from database import get_db_session
from sqlalchemy import text

# --- CONFIGURATION ---
INPUT_FOLDER = os.getenv("INVOICE_INPUT_FOLDER", r"\\BRD-DESKTOP-ELV\storage")

# Output folders for file organization
BASE_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROCESSED_FOLDER = os.path.join(BASE_FOLDER, "Invoices_Processed")
FAILED_FOLDER = os.path.join(BASE_FOLDER, "Invoices_Failed")
LOG_FILE = os.path.join(BASE_FOLDER, "processor.log")

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


def setup_folders():
    """Create necessary folders if they don't exist."""
    for folder in [PROCESSED_FOLDER, FAILED_FOLDER]:
        if not os.path.exists(folder):
            try:
                os.makedirs(folder)
                logger.info(f"Created folder: {folder}")
            except OSError as e:
                logger.error(f"Error creating folder {folder}: {e}")


def extract_invoice_data(pdf_path):
    """
    Extracts Customer Name, Total Value, Invoice Number, Order Number, Area, and Invoice Date from a PDF.
    Also detects Credit Notes (BCRN) and extracts Reference Number.
    """
    data = {
        "filename": os.path.basename(pdf_path),
        "date_processed": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "customer_name": "Unknown",
        "total_value": "0.00",
        "invoice_number": "N/A",
        "order_number": "N/A",
        "invoice_date": "N/A",
        "area": "UNKNOWN",
        "type": "INVOICE",
        "reference_number": None,
        "status": "PENDING"
    }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() + "\n"
            
            # --- PATTERNS CUSTOMIZED FOR YOUR INVOICE FORMAT ---
            
            # Customer Name from "Customer House No:" line
            house_match = re.search(r"Customer\s*House\s*No[:\s]*\d*\s+([A-Z][A-Z0-9\s]+)", text, re.IGNORECASE)
            if house_match:
                customer_name = house_match.group(1).strip()
                customer_name = re.split(r'\s{2,}|Telephone|Customer Street', customer_name)[0].strip()
                if customer_name and len(customer_name) > 2:
                    data["customer_name"] = customer_name
            
            # Fallback: Extract from PDF filename
            if data["customer_name"] == "Unknown":
                filename = os.path.basename(pdf_path)
                name_match = re.search(r"\(QR\)-(.+?)\s*BINV", filename)
                if name_match:
                    data["customer_name"] = name_match.group(1).strip()

            # Invoice Total
            total_match = re.search(r"Invoice\s*Total[:\s]+(?:USD\s*)?([\d,]+\.?\d*)", text, re.IGNORECASE)
            if total_match:
                data["total_value"] = total_match.group(1).strip()

            # Invoice Number - Check for BCRN (Credit Note) or BINV
            # Priority to BCRN to identify Credit Note
            bcrn_match = re.search(r"Invoice\s*No[:\s]*(BCRN[\d]+)", text, re.IGNORECASE)
            if bcrn_match:
                data["invoice_number"] = bcrn_match.group(1).strip()
                data["type"] = "CREDIT_NOTE"
            else:
                 invoice_match = re.search(r"Invoice\s*No[:\s]*([\w]+)", text, re.IGNORECASE)
                 if invoice_match:
                     data["invoice_number"] = invoice_match.group(1).strip()
            
            # Reference Number (for Credit Notes)
            if data["type"] == "CREDIT_NOTE":
                ref_match = re.search(r"Reference\s*No[:\s]*([\w]+)", text, re.IGNORECASE)
                if ref_match:
                    data["reference_number"] = ref_match.group(1).strip()

            # Order Number and Invoice Date from table
            order_header_match = re.search(r"Account\s+Date\s+Order\s+No", text, re.IGNORECASE)
            if order_header_match:
                lines = text.split('\n')
                for i, line in enumerate(lines):
                    if "Account" in line and "Date" in line and "Order" in line:
                        if i + 1 < len(lines):
                            data_line = lines[i+1]
                            # MODIFIED: Capture Account (Customer Number) as first group
                            # Format: Account (Code) | Date | Order No
                            data_match = re.search(r"([\w\d]+)\s+(\d{1,2}/\d{1,2}/\d{4})\s+([\w\d]+)", data_line)
                            if data_match:
                                data["customer_number"] = data_match.group(1).strip() # NEW
                                raw_date = data_match.group(2).strip()
                                try:
                                    dt = datetime.datetime.strptime(raw_date, "%d/%m/%Y")
                                    data["invoice_date"] = dt.strftime("%Y-%m-%d")
                                except ValueError:
                                    data["invoice_date"] = raw_date
                                
                                data["order_number"] = data_match.group(3).strip()
                                break
            
            # Fallback for Order Number
            if data["order_number"] == "N/A":
                # Try finding "Order No:" or "Order Number:" explicitly (Allow alphanumeric)
                order_alt_match = re.search(r"Order\s*(?:No|Number)[:\s]+([\w\d]+)", text, re.IGNORECASE)
                if order_alt_match:
                    data["order_number"] = order_alt_match.group(1).strip()
                else:
                    # Look for standalone number near "Sales Order"
                    # Allow space in order number e.g. "SO 1234"
                    sales_order_match = re.search(r"Sales\s*Order[:\s]+([\w\d\s\-]+)", text, re.IGNORECASE)
                    if sales_order_match:
                        found_order = sales_order_match.group(1).strip()
                        # Clean up if it grabbed too much text
                        if len(found_order) < 20: 
                            data["order_number"] = found_order

            # --- VALIDATION: CLEANUP ORDER NUMBER ---
            # Blacklist of words that are definitely NOT order numbers (detected from user feedback)
            BAD_ORDER_WORDS = ["USD", "ZIG", "ZWG", "ZAR", "EUR", "GBP", "LOUISE", "LOIUSE", "TINROOF", "GREENS"]
            
            raw_order = data["order_number"].upper().strip()
            
            # 1. Check against blacklist
            if raw_order in BAD_ORDER_WORDS:
                data["order_number"] = "N/A"
            
            # 2. Check if it looks like a name (purely alphabetic and length > 3, though some order/inv numbers are alphanumeric)
            # Most order numbers have digits. If it's purely letters and a common word, likely garbage.
            elif raw_order.isalpha() and len(raw_order) > 3:
                 # Heuristic: Assume it's a name if purely alpha. 
                 # Risk: some systems use "ORDABC". But "USD" is caught above.
                 # Let's be conservative and just rely on blacklist + context logic above.
                 # But if it's EXACTLY "USD" or similar, we kill it.
                 pass

            # 3. If it matches the Invoice Total (sometimes it shifts column), clear it
            if data["order_number"] == data["total_value"]:
                data["order_number"] = "N/A"

            
            # Area
            area_match = re.search(r"Customer\s*(?:Area|City)[:\s]+([^\n]+)", text, re.IGNORECASE)
            if area_match:
                area = area_match.group(1).strip()
                area = area.split()[0] if area else "UNKNOWN"
                data["area"] = area.upper()
            
            # Invoice Date from fiscal device timestamp
            date_match = re.search(r"Date:\s*(\d{4}-\d{2}-\d{2})", text, re.IGNORECASE)
            if date_match:
                data["invoice_date"] = date_match.group(1).strip()

    except Exception as e:
        logger.error(f"Error reading {pdf_path}: {e}")
        return None

    return data



def move_file(src_path, dest_folder):
    """Move a file to the destination folder."""
    try:
        filename = os.path.basename(src_path)
        dest_path = os.path.join(dest_folder, filename)
        
        # Handle duplicate filenames by adding timestamp
        if os.path.exists(dest_path):
            name, ext = os.path.splitext(filename)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{name}_{timestamp}{ext}"
            dest_path = os.path.join(dest_folder, filename)
        
        # shutil.move(src_path, dest_path)
        shutil.copy2(src_path, dest_path)  # Copy instead of Move to protect network files
        logger.info(f"Copied {os.path.basename(src_path)} to {dest_folder}")
        return True
    except Exception as e:
        logger.error(f"Error copying {src_path}: {e}")
        return False



def process_invoice_logic(invoice_data):
    """
    Apply business logic for Invoices and Credit Notes.
    Returns True if successful, False otherwise.
    """
    filename = invoice_data['filename']
    
    # CHECK FOR CREDIT NOTE
    if invoice_data.get("type") == "CREDIT_NOTE":
        logger.info(f"  -> Detected Credit Note: {invoice_data['invoice_number']} for {invoice_data['total_value']}")
        
        # Apply Logic
        ref_number = invoice_data.get("reference_number")
        if ref_number:
            linked_invoice = database.get_order_by_invoice_number(ref_number)
            if linked_invoice:
                try:
                    # Parse amounts
                    credit_val = float(invoice_data['total_value'].replace(',', ''))
                    invoice_val = float(linked_invoice['total_value'].replace(',', ''))
                    
                    # Check Full or Partial
                    if credit_val >= invoice_val - 0.01: # Small epsilon for float comparison
                        # Full Credit -> Cancel
                        database.cancel_order(ref_number)
                        invoice_data['status'] = 'PROCESSED' # CN processed
                        logger.info(f"     -> FULL CREDIT: Cancelled Invoice {ref_number}")
                    else:
                        # Partial Credit -> Adjust
                        new_val = invoice_val - credit_val
                        original_val = linked_invoice.get('original_value') or linked_invoice['total_value']
                        database.update_order_value(ref_number, f"{new_val:.2f}", original_val)
                        invoice_data['status'] = 'PROCESSED'
                        logger.info(f"     -> PARTIAL CREDIT: Adjusted Invoice {ref_number} to {new_val:.2f}")
                except ValueError:
                    logger.error("     -> Error parsing values for logic application")
            else:
                logger.warning(f"     -> Linked Invoice {ref_number} NOT FOUND in DB. Marking CN as ORPHAN.")
                invoice_data['status'] = 'ORPHAN'
        else:
            logger.warning("     -> Credit Note has NO Reference Number.")
            invoice_data['status'] = 'INVALID'

    # Save to database (Invoice or Credit Note)
    success = database.add_order(invoice_data)
    if success:
        if invoice_data.get("type") == "INVOICE":
            logger.info(f"  -> Added Invoice {invoice_data['customer_name']} - ${invoice_data['total_value']}")
        # Copy/Move file logic
        # move_file(pdf_path, PROCESSED_FOLDER) # Passed separately if needed, primarily DB op here
        return True
    else:
        logger.warning(f"  -> Duplicate entry (already in DB): {filename}")
        # move_file(pdf_path, PROCESSED_FOLDER)
        return False

def main():
    """Main processing function."""
    logger.info("--- Starting Invoice Processor ---")
    
    # Initialize database
    database.init_db()

    # Input folder check
    if not os.path.exists(INPUT_FOLDER):
        logger.warning(f"Input folder does not exist: {INPUT_FOLDER}")
        logger.info("Please update INPUT_FOLDER in invoice_processor.py to your network path")
        return
    
    # --- AUTO-CLEANUP: Remove records with verified BAD data so they can be re-scanned ---
    # Safety: only delete rows NOT referenced in report_items (already dispatched
    # invoices must never be silently removed, even if their data was bad).
    db = get_db_session()
    try:
        bad_values = ["USD", "ZIG", "ZWG", "LOIUSE", "LOUISE"]

        # Build named parameters for dynamic IN clause
        params = {f'bad_{i}': val for i, val in enumerate(bad_values)}
        placeholders = ','.join([f':bad_{i}' for i in range(len(bad_values))])

        # Delete orders where order_number is in the bad list AND not dispatched
        result = db.execute(text(
            f"DELETE FROM orders WHERE order_number IN ({placeholders}) "
            f"AND NOT EXISTS ("
            f"  SELECT 1 FROM report_items ri "
            f"  WHERE ri.invoice_number = orders.invoice_number"
            f")"
        ), params)
        deleted_count = result.rowcount
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} records with bad Order Numbers (USD/Names). They will be re-scanned.")
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error during auto-cleanup: {e}")
    finally:
        db.close()
    
    # Get already processed filenames from database (including Credit Notes)
    # We fetch ALL orders to avoid re-processing anything
    db = get_db_session()
    try:
        rows = db.execute(text("SELECT filename FROM orders")).mappings().all()
        processed_filenames = {row['filename'] for row in rows}
    finally:
        db.close()
    
    pdf_files = glob.glob(os.path.join(INPUT_FOLDER, "*.pdf"))
    
    # --- DATE FILTER OPTIMIZATION ---
    # Only process files modified on or after January 23, 2026
    cutoff_date = datetime.datetime(2026, 1, 23)
    cutoff_timestamp = cutoff_date.timestamp()
    
    initial_count = len(pdf_files)
    pdf_files = [f for f in pdf_files if os.path.exists(f) and os.path.getmtime(f) >= cutoff_timestamp]
    skipped_count = initial_count - len(pdf_files)
    
    if skipped_count > 0:
        logger.info(f"Skipped {skipped_count} files older than {cutoff_date.strftime('%Y-%m-%d')}")

    if not pdf_files:
        logger.info(f"No PDF files found in {INPUT_FOLDER}")
        return

    new_count = 0
    failed_count = 0
    credit_note_count = 0
    
    for pdf_file in pdf_files:
        filename = os.path.basename(pdf_file)
        
        if filename in processed_filenames:
            logger.debug(f"Skipping already processed: {filename}")
            continue
            
        logger.info(f"Processing: {filename}...")
        invoice_data = extract_invoice_data(pdf_file)
        
        if invoice_data:
            if invoice_data.get("type") == "CREDIT_NOTE":
                credit_note_count += 1
            
            if process_invoice_logic(invoice_data):
                new_count += 1
        else:
            failed_count += 1
            logger.error(f"  -> Failed to extract data from {filename}")
    
    logger.info(f"Done. Processed {new_count} new files ({credit_note_count} Credit Notes), {failed_count} failed.")



if __name__ == "__main__":
    main()

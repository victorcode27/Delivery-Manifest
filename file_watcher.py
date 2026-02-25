"""
Safe File Watcher Service for Invoice Processing
Polls a folder for new PDF files and ensures they're fully written before processing.
"""

import os
import time
import logging
import datetime
from pathlib import Path
from typing import Set, Dict
import invoice_processor

# --- CONFIGURATION ---
# --- CONFIGURATION ---
try:
    from invoice_processor import INPUT_FOLDER as WATCH_FOLDER
except ImportError:
    WATCH_FOLDER = os.getenv("INVOICE_INPUT_FOLDER", r"\\BRD-DESKTOP-ELV\storage")  # Fallback

POLL_INTERVAL = 20  # seconds - Optimized for responsiveness
FILE_STABILITY_CHECKS = 3  # Number of consecutive checks to verify file is stable
FILE_STABILITY_DELAY = 2  # seconds between stability checks

# Logging
BASE_FOLDER = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_FOLDER, "file_watcher.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FileWatcher:
    """Watches a folder for new PDF files and ensures they're fully written."""
    
    def __init__(self, watch_folder: str, poll_interval: int = 30):
        self.watch_folder = Path(watch_folder)
        self.poll_interval = poll_interval
        self.known_files: Set[str] = set()
        self.file_sizes: Dict[str, int] = {}  # Track file sizes for stability check
        self.running = False
        
    def is_file_stable(self, file_path: Path) -> bool:
        """
        Verify that a file has finished writing by checking if its size
        remains constant over multiple checks.
        
        This prevents reading files that are still being written/copied.
        """
        try:
            # Check if file exists and is accessible
            if not file_path.exists():
                return False
            
            # Perform multiple checks to ensure file size is stable
            previous_size = None
            for check in range(FILE_STABILITY_CHECKS):
                try:
                    current_size = file_path.stat().st_size
                    
                    # File size must be > 0 (not empty)
                    if current_size == 0:
                        logger.debug(f"File is empty: {file_path.name}")
                        return False
                    
                    # If we have a previous size, compare
                    if previous_size is not None:
                        if current_size != previous_size:
                            logger.debug(f"File size changed: {file_path.name} ({previous_size} -> {current_size})")
                            return False
                    
                    previous_size = current_size
                    
                    # Wait before next check (except on last iteration)
                    if check < FILE_STABILITY_CHECKS - 1:
                        time.sleep(FILE_STABILITY_DELAY)
                        
                except (PermissionError, OSError) as e:
                    logger.debug(f"Cannot access file (may be locked): {file_path.name} - {e}")
                    return False
            
            # Additional check: Try to open the file exclusively
            # This ensures no other process has it locked
            try:
                # On Windows, opening a file being written will fail
                with open(file_path, 'rb') as f:
                    # Read first few bytes to ensure we can actually access content
                    f.read(1024)
                logger.info(f"[OK] File is stable and ready: {file_path.name} ({previous_size} bytes)")
                return True
            except (PermissionError, OSError) as e:
                logger.debug(f"File is locked or inaccessible: {file_path.name} - {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error checking file stability for {file_path.name}: {e}")
            return False
    
    def scan_folder(self) -> Set[Path]:
        """Scan the watch folder for PDF files."""
        try:
            if not self.watch_folder.exists():
                logger.error(f"Watch folder does not exist: {self.watch_folder}")
                return set()
            
            # Get all PDF files
            pdf_files = set(self.watch_folder.glob("*.pdf"))
            return pdf_files
            
        except Exception as e:
            logger.error(f"Error scanning folder {self.watch_folder}: {e}")
            return set()
    
    def initialize_known_files(self):
        """Initialize the set of known files (files already present when starting)."""
        logger.info("Initializing known files...")
        
        # Get all current PDFs
        current_files = self.scan_folder()
        
        # Import database to check which files are already processed
        import database
        database.init_db()
        
        all_orders = database.get_all_orders(allocated=True) + database.get_all_orders(allocated=False)
        processed_filenames = {order.get("filename") for order in all_orders}
        
        # Mark all existing files as known
        for file_path in current_files:
            self.known_files.add(file_path.name)
        
        logger.info(f"Found {len(current_files)} existing PDF files")
        logger.info(f"Database contains {len(processed_filenames)} processed files")
        logger.info("File watcher initialized. Only NEW files will be processed.")
    
    def process_new_file(self, file_path: Path):
        """Process a newly detected and stable file."""
        try:
            logger.info(f"[PROCESSING] New file: {file_path.name}")
            
            # Extract data using invoice_processor
            invoice_data = invoice_processor.extract_invoice_data(str(file_path))
            
            if invoice_data:
                # Save to database
                import database
                success = database.add_order(invoice_data)
                
                if success:
                    logger.info(f"[ADDED] {invoice_data['customer_name']} - ${invoice_data['total_value']}")
                    return True
                else:
                    logger.warning(f"[DUPLICATE] Already in DB: {file_path.name}")
                    return False
            else:
                logger.error(f"[ERROR] Failed to extract data from: {file_path.name}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {e}")
            return False
    
    def run(self):
        """Main polling loop."""
        logger.info("=" * 60)
        logger.info("Starting File Watcher Service")
        logger.info(f"Watch Folder: {self.watch_folder}")
        logger.info(f"Poll Interval: {self.poll_interval} seconds")
        logger.info(f"File Stability Checks: {FILE_STABILITY_CHECKS} checks x {FILE_STABILITY_DELAY}s")
        logger.info("=" * 60)
        
        # Initialize known files
        self.initialize_known_files()
        
        self.running = True
        logger.info("Watching for new files... (Press Ctrl+C to stop)")
        
        try:
            while self.running:
                # Scan for files
                current_files = self.scan_folder()
                
                # Find new files
                new_files = []
                for file_path in current_files:
                    if file_path.name not in self.known_files:
                        new_files.append(file_path)
                
                # Process new files
                for file_path in new_files:
                    logger.info(f"[NEW] File detected: {file_path.name}")
                    
                    # Check if file is stable (fully written)
                    if self.is_file_stable(file_path):
                        # Process the file
                        success = self.process_new_file(file_path)
                        
                        # Mark as known (whether successful or not, don't reprocess)
                        self.known_files.add(file_path.name)
                    else:
                        logger.info(f"[WAIT] File not ready yet, will retry: {file_path.name}")
                        # Don't add to known_files yet, will check again next iteration
                
                # Sleep until next poll
                time.sleep(self.poll_interval)
                
        except KeyboardInterrupt:
            logger.info("\n[STOP] Stopping file watcher (Ctrl+C pressed)")
            self.running = False
        except Exception as e:
            logger.error(f"Fatal error in polling loop: {e}")
            self.running = False


def main():
    """Entry point for the file watcher service."""
    watcher = FileWatcher(
        watch_folder=WATCH_FOLDER,
        poll_interval=POLL_INTERVAL
    )
    watcher.run()


if __name__ == "__main__":
    main()

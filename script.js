// State
let orders = [];
let reports = [];
let availableInvoices = []; // Invoices loaded from API
let currentUser = null;
let currentUserRole = null; // Fix: Define currentUserRole
let users = [];
let loginIntent = null; // 'manifest' or 'report'
let currentReportView = 'dispatched'; // 'dispatched' or 'outstanding'


// API Configuration
// Use relative paths so fetch() calls work on any host/port (local or Render production).
// "\'"' means all requests go to the same origin serving this page.
const API_URL = '';

// Safety check: Log resolved origin on startup
console.log(`[API Config] Using relative API paths. Page origin: ${window.location.origin}`);

// Error Logger for UI
function logError(msg) {
    console.error(msg);
    let errBox = document.getElementById('debug-error-box');
    if (!errBox) {
        errBox = document.createElement('div');
        errBox.id = 'debug-error-box';
        errBox.style = "position:fixed; bottom:10px; right:10px; background:red; color:white; padding:10px; z-index:99999; max-width:300px; border-radius:5px; font-family:sans-serif; font-size:12px;";
        document.body.appendChild(errBox);
    }
    errBox.innerText = "Error: " + msg;
}

// Global Error Handler to catch unexpected crashes
window.onerror = function (msg, url, lineNo, columnNo, error) {
    logError(`System Error: ${msg} (Line ${lineNo})`);
    return false;
};

// Manifest Number Configuration
const MANIFEST_PREFIX = 'A';
const MANIFEST_START_NUMBER = 35426;

// Get the next available manifest number
function getNextManifestNumber() {
    let lastNumber = localStorage.getItem('lastManifestNumber');

    if (lastNumber === null) {
        // Initialize with starting number
        lastNumber = MANIFEST_START_NUMBER;
    } else {
        lastNumber = parseInt(lastNumber) + 1;
    }

    return MANIFEST_PREFIX + lastNumber;
}

// Save the manifest number when it's used (when printing/generating manifest)
function markManifestNumberAsUsed(manifestNumber) {
    const numericPart = parseInt(manifestNumber.replace(MANIFEST_PREFIX, ''));
    localStorage.setItem('lastManifestNumber', numericPart);
}

// Generate a new manifest number for a new manifest session
function generateNewManifestNumber() {
    const nextNumber = getNextManifestNumber();
    formInputs.manifestNumber.value = nextNumber;
    saveState();
    return nextNumber;
}

// Truck Fleet Data - Will be loaded from settings
// Default data (used only for initial setup)
const DEFAULT_TRUCK_FLEET = [
    { reg: "AEK0269", driver: "John Doe", assistant: "Jane Smith", checker: "Mike Brown" },
    { reg: "ABC1234", driver: "Alice Cooper", assistant: "Bob Marley", checker: "Charlie Watts" },
    { reg: "XYZ9876", driver: "David Gilmour", assistant: "Roger Waters", checker: "Nick Mason" },
    { reg: "DEF4567", driver: "Freddie Mercury", assistant: "Brian May", checker: "Roger Taylor" },
    { reg: "GHI7890", driver: "James Hetfield", assistant: "Lars Ulrich", checker: "Kirk Hammett" }
];

// Settings Data Structure
let settingsData = {
    drivers: [],
    assistants: [],
    checkers: [],
    routes: [],
    trucks: [], // Each truck: { reg, driver, assistant, checker }
    customerRoutes: {} // { "CustomerName": "RouteName", ... }
};

// Load settings from API
async function loadSettings() {
    // Fetches one endpoint and returns parsed JSON, or null on any failure.
    // Never throws — all errors are logged to console only.
    async function safeFetch(url, label) {
        try {
            const res = await fetch(url);
            if (!res.ok) {
                console.error(`[Settings] ${label} returned HTTP ${res.status} ${res.statusText}`);
                return null;
            }
            return await res.json();
        } catch (err) {
            console.error(`[Settings] ${label} request failed: ${err.message}`);
            return null;
        }
    }

    const [driversData, assistantsData, checkersData, routesData, trucksData, custRoutesData] =
        await Promise.all([
            safeFetch(`${API_URL}/settings/drivers`,    'Drivers'),
            safeFetch(`${API_URL}/settings/assistants`, 'Assistants'),
            safeFetch(`${API_URL}/settings/checkers`,   'Checkers'),
            safeFetch(`${API_URL}/settings/routes`,     'Routes'),
            safeFetch(`${API_URL}/trucks`,              'Trucks'),
            safeFetch(`${API_URL}/customer-routes`,     'Customer Routes'),
        ]);

    // Safely apply data — null-safe, falls back to empty collections
    settingsData.drivers        = driversData?.values    ?? [];
    settingsData.assistants     = assistantsData?.values ?? [];
    settingsData.checkers       = checkersData?.values   ?? [];
    settingsData.routes         = routesData?.values     ?? [];
    settingsData.trucks         = trucksData?.trucks     ?? [];
    settingsData.customerRoutes = custRoutesData?.routes ?? {};

    // Only alert the user if every single endpoint failed (server unreachable)
    const allFailed = [driversData, assistantsData, checkersData, routesData, trucksData, custRoutesData]
        .every(d => d === null);

    if (allFailed) {
        console.error('[Settings] All endpoints unreachable — backend may not be running.');
        logError('Connection Failed: Backend unreachable. Ensure the server is running on port 8000.');
    } else {
        console.log('[Settings] Settings loaded from server.');
    }

    // Refresh UI list renders if the settings panel is available
    if (typeof renderSettingsList === 'function') {
        try {
            renderSettingsList('drivers');
            renderSettingsList('assistants');
            renderSettingsList('checkers');
            renderSettingsList('routes');
            renderTrucksList();
        } catch (e) {
            console.warn('[Settings] UI refresh failed:', e);
        }
    }
}


// Save settings - DEPRECATED (We now save per-action)
function saveSettings() {
    // No-op
}

// DOM Elements
const formInputs = {
    manifestNumber: document.getElementById('manifest-number'),
    date: document.getElementById('date'),
    regNumber: document.getElementById('reg-number'),
    driver: document.getElementById('driver'),
    assistant: document.getElementById('assistant'),
    checker: document.getElementById('checker'),
    palletsBrown: document.getElementById('pallets-brown'),
    palletsBlue: document.getElementById('pallets-blue'),
    crates: document.getElementById('crates'),
    mileage: document.getElementById('mileage'),
};

const orderInputs = {
    invoice: document.getElementById('invoice-number'),
    orderNumber: document.getElementById('order-number-manual'),
    invoiceDate: document.getElementById('invoice-date-manual'),
    customer: document.getElementById('customer-name'),
    area: document.getElementById('order-area'),
    sku: document.getElementById('sku'),
    value: document.getElementById('order-value'),
    weight: document.getElementById('weight'),
    volume: document.getElementById('volume'),
};

const tableBody = document.querySelector('#manifest-table tbody');
const totalSkuEl = document.getElementById('total-sku');
const totalValueEl = document.getElementById('total-value');
const totalWeightEl = document.getElementById('total-weight');
const orderCountEl = document.getElementById('order-count');

// Initialization
document.addEventListener('DOMContentLoaded', async () => {
    // WAIT for settings to load first (defensive against API failures)
    try {
        await loadSettings();
    } catch (e) {
        console.warn("Settings failed to load, continuing with empty defaults");
        logError("Settings failed, continuing with empty defaults");
    }

    // Now initialize UI with loaded (or empty) settings
    await loadState();
    setDefaultDate();
    initManifestNumber(); // Auto-generate manifest number if not set
    initTruckDropdown(); // Initialize truck list - now has settings data
    initPersonnelDropdowns(); // Initialize driver, assistant, checker dropdowns - now has settings data
    renderTable();
    setupEventListeners();

    // Explicitly call handleTruckChange if there's a saved truck value to populate names
    if (formInputs.regNumber.value) {
        handleTruckChange();
    }

    // Initial Landing Page
    initUsers();
    showLandingPage();

    // Initialize DEV_MODE if available
    if (window.DevMode) {
        window.DevMode.init();
    }
});

// User Management Initialization
function initUsers() {
    // Users are now managed by the backend database
    // We no longer store users in localStorage
    // Just ensure users array is empty (it's only used for legacy compatibility)
    users = [];
}

function saveUsers() {
    // No-op - users are now saved in the database
    console.log('Users are now managed by the backend database');
}

// Initialize manifest number - generate new one if not already set
function initManifestNumber() {
    if (!formInputs.manifestNumber.value) {
        formInputs.manifestNumber.value = getNextManifestNumber();
        saveState();
    }
}

function initTruckDropdown() {
    const select = formInputs.regNumber;
    // Clear existing options except the first one (if any) or just rebuild
    select.innerHTML = '<option value="">Select Truck</option>';

    settingsData.trucks.forEach(truck => {
        const option = document.createElement('option');
        option.value = truck.reg;
        option.textContent = truck.reg;
        select.appendChild(option);
    });
}

// Initialize Driver, Assistant, Checker dropdowns with values from settings
function initPersonnelDropdowns() {
    // Get personnel from settings
    const drivers = settingsData.drivers || [];
    const assistants = settingsData.assistants || [];
    const checkers = settingsData.checkers || [];

    // console.log("Initializing Dropdowns:", { drivers, assistants, checkers });

    // Populate Driver dropdown
    const driverSelect = formInputs.driver;
    const currentDriver = driverSelect.value; // Store current selection
    driverSelect.innerHTML = '<option value="">Select Driver</option>';
    drivers.forEach(name => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        if (name === currentDriver) option.selected = true;
        driverSelect.appendChild(option);
    });

    // Populate Assistant dropdown
    const assistantSelect = formInputs.assistant;
    const currentAssistant = assistantSelect.value;
    assistantSelect.innerHTML = '<option value="">Select Assistant</option>';
    assistants.forEach(name => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        if (name === currentAssistant) option.selected = true;
        assistantSelect.appendChild(option);
    });

    // Populate Checker dropdown
    const checkerSelect = formInputs.checker;
    const currentChecker = checkerSelect.value;
    checkerSelect.innerHTML = '<option value="">Select Checker</option>';
    checkers.forEach(name => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        if (name === currentChecker) option.selected = true;
        checkerSelect.appendChild(option);
    });
}

function handleTruckChange() {
    const selectedReg = formInputs.regNumber.value;
    const truck = settingsData.trucks.find(t => t.reg === selectedReg);

    if (truck) {
        formInputs.driver.value = truck.driver;
        formInputs.assistant.value = truck.assistant;
        formInputs.checker.value = truck.checker;
        saveState(); // Save auto-filled values
    } else if (selectedReg === "") {
        // Optional: clear fields if "Select Truck" is chosen?
        // formInputs.driver.value = "";
        // formInputs.assistant.value = "";
        // formInputs.checker.value = "";
    }
}

function setupEventListeners() {
    try {
        document.getElementById('add-order-btn').addEventListener('click', addOrder);
        document.getElementById('reset-btn').addEventListener('click', resetSystem);
        document.getElementById('print-btn').addEventListener('click', showPreviewModal);
        document.getElementById('view-reports-btn').addEventListener('click', showReports);
        document.getElementById('close-modal-btn').addEventListener('click', hideReports);
        document.getElementById('filter-btn').addEventListener('click', filterReports);
        document.getElementById('export-report-btn').addEventListener('click', showExportOptions);
        // document.getElementById('clear-history-btn').addEventListener('click', clearReportHistory);
        document.getElementById('logout-btn').addEventListener('click', handleLogout);

        // New navigation buttons
        document.getElementById('dispatch-report-btn').addEventListener('click', () => {
            window.location.href = 'dispatch_report.html';
        });
        document.getElementById('outstanding-orders-btn').addEventListener('click', () => {
            window.location.href = 'outstanding_orders.html';
        });

        // Call secondary listeners
        initSecondaryListeners();
    } catch (error) {
        console.error("Error in setupEventListeners:", error);
        // Try to continue with secondary listeners even if primary setup fails
        try {
            initSecondaryListeners();
        } catch (e) {
            console.error("Error in initSecondaryListeners:", e);
        }
    }
}

// ... (previous functions) ...

// =============================================
// MANUAL ENTRY & RESTORE FUNCTIONS
// =============================================

function openManualEntryModal() {
    try {
        const modal = document.getElementById('manual-entry-modal');
        if (!modal) {
            alert('Error: Manual Entry Modal not found!');
            return;
        }
        modal.classList.remove('hidden');
        modal.classList.add('visible');
        // Default to manual entry tab
        switchManualModalTab('manual');
    } catch (e) {
        console.error(e);
        alert('Error opening modal: ' + e.message);
    }
}

function closeManualEntryModal() {
    const modal = document.getElementById('manual-entry-modal');
    modal.classList.remove('visible');
    modal.classList.add('hidden');
}

function switchManualModalTab(tab) {
    const manualBtn = document.getElementById('tab-manual-entry-btn');
    const restoreBtn = document.getElementById('tab-restore-history-btn');
    const manualView = document.getElementById('manual-entry-view');
    const restoreView = document.getElementById('restore-history-view');

    if (tab === 'manual') {
        manualBtn.classList.add('active');
        restoreBtn.classList.remove('active');
        manualView.classList.remove('hidden');
        restoreView.classList.add('hidden');
    } else {
        manualBtn.classList.remove('active');
        restoreBtn.classList.add('active');
        manualView.classList.add('hidden');
        restoreView.classList.remove('hidden');
        // Focus search box
        document.getElementById('restore-search-input').focus();
    }
}

async function submitManualEntry() {
    const invoiceNum = document.getElementById('manual-invoice-number').value.trim();
    const orderNum = document.getElementById('manual-order-number').value.trim();
    const customer = document.getElementById('manual-customer-name').value.trim();
    const customerNumber = document.getElementById('manual-customer-number').value.trim();
    const value = document.getElementById('manual-total-value').value;
    const area = document.getElementById('manual-area').value.trim();

    if (!invoiceNum || !orderNum || !customer || !value) {
        alert("Please fill in all required fields marked with *");
        return;
    }

    try {
        const response = await fetch(`${API_URL}/invoices/manual`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                invoice_number: invoiceNum,
                order_number: orderNum,
                customer_name: customer,
                customer_number: customerNumber,
                total_value: value,
                area: area || "UNKNOWN"
            })
        });

        if (response.ok) {
            alert("Invoice added successfully!");
            // Clear inputs
            document.getElementById('manual-invoice-number').value = '';
            document.getElementById('manual-order-number').value = '';
            document.getElementById('manual-customer-name').value = '';
            document.getElementById('manual-customer-number').value = '';
            document.getElementById('manual-total-value').value = '';
            document.getElementById('manual-area').value = '';

            // Refresh main list if viewing it
            closeManualEntryModal();
            // Optional: trigger reload of invoices in main view if needed
            // loadSystemInvoices(); 
        } else {
            const err = await response.json();
            alert("Error adding invoice: " + (err.detail || "Unknown error"));
        }
    } catch (e) {
        console.error("Error submitting manual entry:", e);
        alert("Connection failed. See console for details.");
    }
}

let restoreDebounceTimer;
function searchRestoreHistory(e) {
    clearTimeout(restoreDebounceTimer);
    const query = e.target.value.trim();

    if (query.length < 2) {
        document.getElementById('restore-list').innerHTML = '';
        return;
    }

    restoreDebounceTimer = setTimeout(async () => {
        try {
            const response = await fetch(`${API_URL}/invoices/search?q=${encodeURIComponent(query)}`);
            const data = await response.json();
            renderRestoreTable(data.results || []);
        } catch (e) {
            console.error("Search failed:", e);
        }
    }, 300);
}

function renderRestoreTable(results) {
    const list = document.getElementById('restore-list');
    list.innerHTML = '';

    if (results.length === 0) {
        list.innerHTML = '<tr><td colspan="5" style="text-align:center;">No matching invoices found</td></tr>';
        return;
    }

    results.forEach(item => {
        const tr = document.createElement('tr');
        const statusClass = item.is_allocated ? 'text-green-600' : 'text-orange-500';
        const statusText = item.is_allocated ? 'Dispatched' : 'Pending';

        // Only allow restoring if it IS allocated (dispatched)
        // If it's already pending, no need to restore, but we show it for clarity
        const disabledAttr = !item.is_allocated ? 'disabled' : '';
        const checkboxHtml = item.is_allocated
            ? `<input type="checkbox" class="restore-checkbox" value="${item.filename}">`
            : '-';

        tr.innerHTML = `
            <td>${checkboxHtml}</td>
            <td>${item.invoice_number}</td>
            <td>${item.customer_name}</td>
            <td class="${statusClass}">${statusText}</td>
            <td>${item.date_processed.split(' ')[0]}</td>
        `;
        list.appendChild(tr);
    });

    // Re-attach checkbox listener for enabling button
    document.querySelectorAll('.restore-checkbox').forEach(cb => {
        cb.addEventListener('change', updateRestoreButtonState);
    });
}

function updateRestoreButtonState() {
    const anyChecked = document.querySelectorAll('.restore-checkbox:checked').length > 0;
    document.getElementById('restore-btn').disabled = !anyChecked;
}

function toggleSelectAllRestore(e) {
    const checked = e.target.checked;
    document.querySelectorAll('.restore-checkbox').forEach(cb => {
        cb.checked = checked;
    });
    updateRestoreButtonState();
}

async function restoreSelectedInvoices() {
    const selectedFilenames = Array.from(document.querySelectorAll('.restore-checkbox:checked'))
        .map(cb => cb.value);

    if (selectedFilenames.length === 0) return;

    if (!confirm(`Are you sure you want to restore ${selectedFilenames.length} invoices to Pending status?`)) return;

    try {
        const response = await fetch(`${API_URL}/invoices/restore`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filenames: selectedFilenames })
        });

        if (response.ok) {
            const res = await response.json();
            alert(res.message);
            // Clear search results
            document.getElementById('restore-list').innerHTML = '';
            document.getElementById('restore-search-input').value = '';
            document.getElementById('restore-btn').disabled = true;
            closeManualEntryModal();
        } else {
            alert("Failed to restore invoices.");
        }
    } catch (e) {
        console.error("Restore failed:", e);
        alert("Connection error occurred.");
    }
}

function initSecondaryListeners() {
    // Export Options listeners
    document.getElementById('close-export-options-btn').addEventListener('click', hideExportOptions);
    document.getElementById('export-download-btn').addEventListener('click', () => {
        hideExportOptions();
        exportDispatchReport();
    });
    document.getElementById('export-print-preview-btn').addEventListener('click', showReportPrintPreview);

    // Print Preview listeners
    document.getElementById('close-report-preview-btn').addEventListener('click', hideReportPreview);
    document.getElementById('cancel-report-print-btn').addEventListener('click', hideReportPreview);
    document.getElementById('confirm-report-print-btn').addEventListener('click', () => window.print());

    // Search filter listeners
    document.getElementById('report-search').addEventListener('input', handleSearchInput);
    document.getElementById('clear-search-btn').addEventListener('click', clearSearch);

    // View toggle listeners
    document.getElementById('view-dispatched-btn').addEventListener('click', () => switchReportView('dispatched'));
    document.getElementById('view-outstanding-btn').addEventListener('click', () => switchReportView('outstanding'));

    // Preview modal listeners
    document.getElementById('close-preview-modal-btn').addEventListener('click', hidePreviewModal);
    document.getElementById('cancel-print-btn').addEventListener('click', hidePreviewModal);
    document.getElementById('confirm-print-btn').addEventListener('click', confirmAndPrint);

    // Invoice modal listeners
    document.getElementById('load-invoices-btn').addEventListener('click', openInvoiceModal);
    // Second button is optional (may not exist if Route/Area field was removed)
    const loadBtn2 = document.getElementById('load-invoices-btn-2');
    if (loadBtn2) loadBtn2.addEventListener('click', openInvoiceModal);
    document.getElementById('close-invoice-modal-btn').addEventListener('click', closeInvoiceModal);
    document.getElementById('add-selected-btn').addEventListener('click', addSelectedInvoices);
    document.getElementById('select-all-invoices').addEventListener('change', toggleSelectAll);
    document.getElementById('refresh-invoices-btn').addEventListener('click', refreshInvoices);

    // Settings modal listeners
    document.getElementById('settings-btn').addEventListener('click', openSettingsModal);
    document.getElementById('close-settings-modal-btn').addEventListener('click', closeSettingsModal);
    document.getElementById('close-settings-btn').addEventListener('click', closeSettingsModal);

    // Settings tabs
    document.querySelectorAll('.settings-tab').forEach(tab => {
        tab.addEventListener('click', handleSettingsTabClick);
    });

    // Settings add buttons
    document.getElementById('add-driver-btn').addEventListener('click', () => addSettingsItem('drivers'));
    document.getElementById('add-assistant-btn').addEventListener('click', () => addSettingsItem('assistants'));
    document.getElementById('add-checker-btn').addEventListener('click', () => addSettingsItem('checkers'));
    document.getElementById('add-route-btn').addEventListener('click', () => addSettingsItem('routes'));
    document.getElementById('add-truck-btn').addEventListener('click', addTruck);
    document.getElementById('add-user-btn').addEventListener('click', addUser);

    // User Settings Tab Click
    document.getElementById('tab-btn-users').addEventListener('click', renderUsersList);

    // Header inputs save on change
    Object.values(formInputs).forEach(input => {
        input.addEventListener('change', saveState);
    });

    // Special listener for truck select
    formInputs.regNumber.addEventListener('change', handleTruckChange);

    // Load areas on startup
    fetchAreas();

    // Landing & Auth Listeners
    document.getElementById('landing-manifest-btn').addEventListener('click', () => openLoginModal('manifest'));
    document.getElementById('landing-report-btn').addEventListener('click', () => openLoginModal('report'));
    document.getElementById('login-submit-btn').addEventListener('click', handleLogin);
    document.getElementById('close-login-btn').addEventListener('click', () => {
        document.getElementById('login-modal').classList.add('hidden');
        document.getElementById('login-modal').classList.remove('visible');
    });
    // Allow Enter key on password/username input
    document.getElementById('login-password').addEventListener('keyup', (e) => {
        if (e.key === 'Enter') handleLogin();
    });
    document.getElementById('login-username').addEventListener('keyup', (e) => {
        if (e.key === 'Enter') document.getElementById('login-password').focus();
    });
}

function setDefaultDate() {
    // Always set to current date (read-only field)
    formInputs.date.valueAsDate = new Date();
}

// Logic
function addOrder() {
    const order = {
        id: Date.now(),
        invoice: orderInputs.invoice.value.trim(),
        orderNumber: orderInputs.orderNumber.value.trim() || '',
        invoiceDate: orderInputs.invoiceDate.value || new Date().toISOString().split('T')[0],
        customer: orderInputs.customer.value.trim(),
        area: orderInputs.area.value.trim(),
        sku: parseInt(orderInputs.sku.value) || 0,
        value: parseFloat(orderInputs.value.value) || 0,
        weight: parseFloat(orderInputs.weight.value) || 0,
        volume: parseFloat(orderInputs.volume.value) || 0,
        timestamp: new Date().toISOString()
    };

    if (!order.invoice || !order.customer) {
        alert('Please fill in at least Invoice Number and Customer Name.');
        return;
    }

    orders.push(order);
    clearOrderInputs();
    saveState();
    renderTable();
}

async function removeOrder(id) {
    if (confirm('Remove this order?')) {
        // Find the order to get its filename
        const order = orders.find(o => o.id === id);
        if (!order) return;

        // Call API to remove from staging
        try {
            const response = await fetch(`${API_URL}/manifest/remove`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Username': currentUser || 'anonymous'
                },
                body: JSON.stringify({ filenames: [order.invoice] })
            });

            if (!response.ok) {
                console.warn('Could not remove invoice from staging');
            }
        } catch (error) {
            console.error('Error removing from staging:', error);
        }

        // Reload manifest from API to refresh the list
        await renderTable();
    }
}

function clearOrderInputs() {
    orderInputs.invoice.value = '';
    orderInputs.orderNumber.value = '';
    orderInputs.invoiceDate.value = '';
    orderInputs.customer.value = '';
    orderInputs.sku.value = '0';
    orderInputs.value.value = '0';
    orderInputs.weight.value = '0';
    orderInputs.volume.value = '0';

    // Keep area as it might be the same for the route
    // orderInputs.area.value = ''; 
    orderInputs.invoice.focus();
}

async function renderTable() {
    // Reload from API first to ensure we have latest data
    await loadCurrentManifestFromAPI();

    tableBody.innerHTML = '';
    let tSku = 0, tValue = 0, tWeight = 0;

    orders.forEach(order => {
        tSku += order.sku;
        tValue += order.value;
        tWeight += order.weight;

        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${order.invoice}</td>
            <td>${order.customer}</td>
            <td>${order.area}</td>
            <td>${order.sku}</td>
            <td>$${order.value.toFixed(2)}</td>
            <td>${order.weight}</td>
            <td>
                <button onclick="removeOrder(${order.id})" class="btn-icon text-red-500">
                    <i data-lucide="trash-2"></i>
                </button>
            </td>
        `;
        tableBody.appendChild(row);
    });

    totalSkuEl.textContent = tSku;
    totalValueEl.textContent = '$' + tValue.toFixed(2);
    totalWeightEl.textContent = tWeight.toFixed(1);
    orderCountEl.textContent = `${orders.length} Invoices`;

    lucide.createIcons();
}

function resetSystem() {
    if (confirm('Are you sure you want to reset everything? This will clear all current data.')) {
        orders = [];
        // Clear all inputs
        Object.values(formInputs).forEach(input => input.value = '');

        // Reset specific defaults
        setDefaultDate();
        formInputs.palletsBrown.value = 0;
        formInputs.palletsBlue.value = 0;
        formInputs.crates.value = 0;
        formInputs.mileage.value = 0;

        // Ensure dropdown is reset to default
        formInputs.regNumber.value = "";

        // Generate new manifest number for the new session
        formInputs.manifestNumber.value = getNextManifestNumber();

        saveState();
        renderTable();
    }
}

// Persistence
function saveState() {
    // Save form header data only (not orders - those come from API)
    const state = {
        header: {}
    };
    Object.keys(formInputs).forEach(key => {
        state.header[key] = formInputs[key].value;
    });
    localStorage.setItem('manifestAppState', JSON.stringify(state));
}

async function loadState() {
    // Load header data from localStorage
    const saved = localStorage.getItem('manifestAppState');
    if (saved) {
        const state = JSON.parse(saved);
        if (state.header) {
            Object.keys(formInputs).forEach(key => {
                if (state.header[key]) formInputs[key].value = state.header[key];
            });
        }
    }

    // Load current manifest from API (replaces localStorage orders)
    await loadCurrentManifestFromAPI();

    const savedReports = localStorage.getItem('manifestReports');
    if (savedReports) {
        reports = JSON.parse(savedReports);
    }
}

// NEW: Load current manifest from staging API
async function loadCurrentManifestFromAPI() {
    // CLEAR FIRST - prevent showing stale data if API fails
    orders = [];

    try {
        const response = await fetch(`${API_URL}/manifest/current`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'X-Username': currentUser || 'anonymous'
            }
        });

        if (!response.ok) {
            console.warn('Could not load current manifest from API');
            // orders already cleared above
            return;
        }

        const data = await response.json();
        const invoices = data.invoices || [];

        // Convert API invoice format to local orders format
        orders = invoices.map(inv => ({
            id: inv.id || Date.now() + Math.random(),
            invoice: inv.invoice_number || inv.filename,
            orderNumber: inv.order_number || '',
            customerNumber: inv.customer_number || 'N/A',
            invoiceDate: inv.invoice_date || 'N/A',
            customer: inv.customer_name,
            area: inv.area || 'UNKNOWN',
            sku: 0, // Default
            value: parseFloat(String(inv.total_value).replace(/[^0-9.-]/g, '')) || 0,
            weight: 0, // Default
            volume: 0, // Default
            timestamp: inv.date_processed || new Date().toISOString()
        }));

        console.log(`Loaded ${orders.length} invoices from current manifest`);
    } catch (error) {
        console.error('Error loading current manifest:', error);
        // orders already cleared above
    }
}

async function saveReport(manifestData) {
    try {
        const response = await fetch(`${API_URL}/reports`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(manifestData)
        });

        if (response.ok) {
            console.log("Report saved to database");
            // If the reports modal is open, refresh it
            if (!document.getElementById('reports-modal').classList.contains('hidden')) {
                renderReports();
            }
        } else {
            console.error("Failed to save report to database");
        }
    } catch (e) {
        console.error("Error saving report:", e);
    }
}

// Reports
let filteredReportData = []; // Store filtered data for export

function showReports() {
    const modal = document.getElementById('reports-modal');
    modal.classList.remove('hidden');
    modal.classList.add('visible');

    // Set default date range to last 7 days (more useful than just today)
    const today = new Date();
    const sevenDaysAgo = new Date(today);
    sevenDaysAgo.setDate(today.getDate() - 7);

    document.getElementById('report-date-from').valueAsDate = sevenDaysAgo;
    document.getElementById('report-date-to').valueAsDate = today;

    renderReports();
}

function hideReports() {
    const modal = document.getElementById('reports-modal');
    modal.classList.remove('visible');
    modal.classList.add('hidden');

    // If guest, return to landing page
    if (currentUserRole === 'guest') {
        showLandingPage();
    }
}

async function renderReports() {
    const list = document.getElementById('reports-list');
    list.innerHTML = '';

    const dateFrom = document.getElementById('report-date-from').value;
    const dateTo = document.getElementById('report-date-to').value;
    const searchTerm = document.getElementById('report-search').value.trim();

    // Fetch dispatched invoices from new endpoint
    try {
        const params = new URLSearchParams();
        if (dateFrom) params.append('date_from', dateFrom);
        if (dateTo) params.append('date_to', dateTo);
        if (searchTerm) params.append('search', searchTerm);

        const url = `${API_URL}/reports/dispatched?${params.toString()}`;
        console.log("=== FETCHING DISPATCHED REPORTS ===");
        console.log("URL:", url);

        const response = await fetch(url);
        const data = await response.json();

        console.log("=== DISPATCHED REPORTS DATA ===");
        console.log("Total invoices:", data.total);
        console.log("Invoices returned:", data.invoices ? data.invoices.length : 0);

        // Transform to display format
        filteredReportData = (data.invoices || []).map(inv => ({
            invoice: inv.invoice_number || 'N/A',
            orderNumber: inv.order_number || 'N/A',
            manifest: inv.manifest_number || 'N/A',
            truckReg: inv.reg_number || 'N/A',
            customer: inv.customer_name || 'N/A',
            customerNumber: inv.customer_number || 'N/A',
            invoiceDate: inv.invoice_date || 'N/A',
            dateDispatched: inv.date_dispatched || 'N/A',
            driver: inv.driver || 'N/A',
            assistant: inv.assistant || 'N/A',
            checker: inv.checker || 'N/A'
        }));

    } catch (e) {
        console.error("Error fetching dispatched reports:", e);
        list.innerHTML = '<tr><td colspan="11" style="text-align:center;">Error loading reports</td></tr>';
        return;
    }

    // Sort by date (newest first)
    filteredReportData.sort((a, b) => new Date(b.dateDispatched) - new Date(a.dateDispatched));

    // Render rows
    if (filteredReportData.length === 0) {
        const message = searchTerm
            ? 'No invoices match your search criteria.'
            : 'No dispatched orders found for the selected date range.';
        list.innerHTML = `<tr><td colspan="11" style="text-align:center; color: #64748b; padding: 2rem;">${message}</td></tr>`;
    } else {
        filteredReportData.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong>${row.invoice}</strong></td>
                <td>${row.orderNumber}</td>
                <td>${row.manifest}</td>
                <td>${row.truckReg}</td>
                <td>${row.customer}</td>
                <td>${row.customerNumber}</td>
                <td>${row.invoiceDate}</td>
                <td>${formatDate(row.dateDispatched)}</td>
                <td>${row.driver}</td>
                <td>${row.assistant}</td>
                <td>${row.checker}</td>
            `;
            list.appendChild(tr);
        });
    }

    // Update summary
    const summaryEl = document.getElementById('report-summary');
    const countEl = document.getElementById('report-count');

    if (filteredReportData.length > 0 && (dateFrom || dateTo)) {
        summaryEl.classList.remove('hidden');
        const fromText = dateFrom ? formatDate(dateFrom) : 'Beginning';
        const toText = dateTo ? formatDate(dateTo) : 'Today';
        summaryEl.innerHTML = `
            <span class="report-summary-text">Dispatch Report: ${fromText} to ${toText}</span>
            <span class="report-summary-count">${filteredReportData.length} invoices</span>
        `;
    } else {
        summaryEl.classList.add('hidden');
    }

    countEl.textContent = `${filteredReportData.length} invoices found`;

    lucide.createIcons();
}

function renderManifestView(manifest) {
    const list = document.getElementById('report-list');
    const summaryEl = document.getElementById('report-summary');

    // Update summary with Manifest Details
    summaryEl.classList.remove('hidden');
    summaryEl.innerHTML = `
        <div style="display:flex; flex-direction:column; gap:0.5rem;">
            <div style="font-size: 1.25rem; font-weight: bold; color: #1e293b;">
                Manifest: ${manifest.manifest_number}
            </div>
            <div style="display:grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; font-size: 0.9rem; color: #64748b;">
                <div><strong>Driver:</strong> ${manifest.driver || 'N/A'}</div>
                <div><strong>Truck:</strong> ${manifest.reg_number || 'N/A'}</div>
                <div><strong>Date:</strong> ${formatDate(manifest.date)}</div>
                <div><strong>Invoices:</strong> ${manifest.invoices.length}</div>
            </div>
            <div style="font-size: 0.8rem; margin-top: 0.5rem; color: #94a3b8;">
                <button onclick="renderReports()" style="background:none; border:none; text-decoration:underline; cursor:pointer; color: #3b82f6;">
                    &larr; Back to all reports
                </button>
            </div>
        </div>
    `;

    // Render Invoices for this Manifest
    list.innerHTML = '';

    if (!manifest.invoices || manifest.invoices.length === 0) {
        list.innerHTML = '<tr><td colspan="10" style="text-align:center;">No invoices found for this manifest.</td></tr>';
        return;
    }

    manifest.invoices.forEach(row => {
        const tr = document.createElement('tr');
        // Handle property name differences (report_items use underscores usually vs memory naming)
        // Adjusting based on DB schema: invoice_number, order_number etc.
        tr.innerHTML = `
            <td><strong>${row.invoice_number || 'N/A'}</strong></td>
            <td>${row.order_number || 'N/A'}</td>
            <td>${manifest.manifest_number}</td>
            <td>${manifest.reg_number || 'N/A'}</td>
            <td>${row.customer_name || 'N/A'}</td>
            <td>${row.customer_number || 'N/A'}</td>
            <td>${row.invoice_date || 'N/A'}</td>
            <td>${formatDate(manifest.date)}</td>
            <td>${manifest.driver || 'N/A'}</td>
            <td>${manifest.assistant || 'N/A'}</td>
            <td>${manifest.checker || 'N/A'}</td>
        `;
        list.appendChild(tr);
    });

    lucide.createIcons();
}

function filterReports() {
    renderReports();
}

// Clear all report history
function clearReportHistory() {
    if (confirm('Are you sure you want to clear all report history? This cannot be undone.')) {
        reports = [];
        localStorage.removeItem('manifestReports');
        renderReports();
        alert('Report history has been cleared.');
    }
}

// Search filter handlers
async function handleSearchInput(e) {
    const searchTerm = e.target.value.trim();
    const clearBtn = document.getElementById('clear-search-btn');

    // Show/hide clear button
    if (searchTerm) {
        clearBtn.classList.remove('hidden');
    } else {
        clearBtn.classList.add('hidden');
    }

    // Check if it's a Manifest Number (simple heuristic or API check)
    // If it starts with 'M' and has digits, or just try to find it via API
    if (searchTerm.length > 2) {
        try {
            const response = await fetch(`${API_URL}/manifests/search/query?q=${encodeURIComponent(searchTerm)}`);
            const data = await response.json();

            if (data.match) {
                // Found a direct manifest match!
                renderManifestView(data.manifest);
                return;
            }
        } catch (err) {
            console.warn("Manifest search check failed", err);
        }
    }

    // Re-render with search filter (standard list filtering)
    renderReports();
}

function clearSearch() {
    const searchInput = document.getElementById('report-search');
    searchInput.value = '';
    document.getElementById('clear-search-btn').classList.add('hidden');
    renderReports();
}

// Switch between Dispatched and Outstanding report views
async function switchReportView(view) {
    currentReportView = view;

    // Update button active states
    const dispatchedBtn = document.getElementById('view-dispatched-btn');
    const outstandingBtn = document.getElementById('view-outstanding-btn');

    if (view === 'dispatched') {
        dispatchedBtn.classList.add('active');
        outstandingBtn.classList.remove('active');
        // Render dispatched reports
        renderReports();
    } else {
        dispatchedBtn.classList.remove('active');
        outstandingBtn.classList.add('active');
        // Render outstanding orders
        await renderOutstandingOrders();
    }
}

// Render Outstanding Orders in the modal
async function renderOutstandingOrders() {
    const list = document.getElementById('reports-list');
    list.innerHTML = '';

    try {
        const response = await fetch(`${API_URL}/reports/outstanding`);
        const data = await response.json();

        console.log("=== OUTSTANDING ORDERS DATA ===");
        console.log("Total orders:", data.count);

        // Transform to display format
        filteredReportData = (data.orders || []).map(order => ({
            invoice: order.invoice_number || 'N/A',
            orderNumber: order.order_number || 'N/A',
            manifest: 'Not Dispatched',
            truckReg: 'N/A',
            customer: order.customer_name || 'N/A',
            customerNumber: order.customer_number || 'N/A',
            invoiceDate: order.invoice_date || 'N/A',
            dateDispatched: 'N/A',
            driver: 'N/A',
            assistant: 'N/A',
            checker: 'N/A'
        }));

    } catch (e) {
        console.error("Error fetching outstanding orders:", e);
        list.innerHTML = '<tr><td colspan="11" style="text-align:center;">Error loading outstanding orders</td></tr>';
        return;
    }

    // Render rows
    if (filteredReportData.length === 0) {
        list.innerHTML = '<tr><td colspan="11" style="text-align:center; color: #64748b; padding: 2rem;">No outstanding orders found.</td></tr>';
    } else {
        filteredReportData.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong>${row.invoice}</strong></td>
                <td>${row.orderNumber}</td>
                <td>${row.manifest}</td>
                <td>${row.truckReg}</td>
                <td>${row.customer}</td>
                <td>${row.customerNumber}</td>
                <td>${row.invoiceDate}</td>
                <td>${row.dateDispatched}</td>
                <td>${row.driver}</td>
                <td>${row.assistant}</td>
                <td>${row.checker}</td>
            `;
            list.appendChild(tr);
        });
    }

    // Update summary
    const summaryEl = document.getElementById('report-summary');
    const countEl = document.getElementById('report-count');

    summaryEl.classList.remove('hidden');
    summaryEl.innerHTML = `
        <span class="report-summary-text">Outstanding Orders</span>
        <span class="report-summary-count">${filteredReportData.length} orders</span>
    `;

    countEl.textContent = `${filteredReportData.length} orders found`;

    lucide.createIcons();
}

// Export Dispatch Report to Excel
async function exportDispatchReport() {
    if (filteredReportData.length === 0) {
        alert('No data to export. Please generate a report first.');
        return;
    }

    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Dispatch Report');

    // Set column widths
    sheet.columns = [
        { header: 'Invoice #', key: 'invoice', width: 15 },
        { header: 'Order #', key: 'orderNumber', width: 15 },
        { header: 'Manifest #', key: 'manifest', width: 15 },
        { header: 'Truck Reg', key: 'truckReg', width: 15 },
        { header: 'Customer Name', key: 'customer', width: 35 },
        { header: 'Customer #', key: 'customerNumber', width: 15 },
        { header: 'Invoice Date', key: 'invoiceDate', width: 15 }, // Added Invoice Date
        { header: 'Date Dispatched', key: 'date', width: 15 },
        { header: 'Driver', key: 'driver', width: 20 },
        { header: 'Assistant', key: 'assistant', width: 20 },
        { header: 'Checker', key: 'checker', width: 20 }
    ];

    // Title row
    const dateFrom = document.getElementById('report-date-from').value;
    const dateTo = document.getElementById('report-date-to').value;
    const fromText = dateFrom ? formatDate(dateFrom) : 'Beginning';
    const toText = dateTo ? formatDate(dateTo) : 'Today';

    sheet.mergeCells('A1:G1');
    sheet.getCell('A1').value = 'BRD DISTRIBUTION - DISPATCH REPORT';
    sheet.getCell('A1').font = { bold: true, size: 16 };
    sheet.getCell('A1').alignment = { horizontal: 'center' };

    sheet.mergeCells('A2:G2');
    sheet.getCell('A2').value = `Period: ${fromText} to ${toText}`;
    sheet.getCell('A2').font = { size: 12 };
    sheet.getCell('A2').alignment = { horizontal: 'center' };

    // Header row
    const headerRow = sheet.getRow(4);
    headerRow.values = ['Invoice #', 'Order #', 'Manifest #', 'Truck Reg', 'Customer Name', 'Customer #', 'Invoice Date', 'Date Dispatched', 'Driver Name', 'Driver Assistant', 'Checker Name'];
    headerRow.eachCell((cell) => {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4472C4' } };
        cell.font = { color: { argb: 'FFFFFFFF' }, bold: true };
        cell.alignment = { horizontal: 'center' };
        cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
    });

    // Data rows
    filteredReportData.forEach(row => {
        const dataRow = sheet.addRow([
            row.invoice,
            row.orderNumber,
            row.manifest || 'N/A',
            row.truckReg || 'N/A',
            row.customer,
            row.customerNumber,
            row.invoiceDate, // Added Invoice Date
            formatDate(row.dateDispatched),
            row.driver,
            row.assistant,
            row.checker
        ]);
        dataRow.eachCell((cell) => {
            cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
        });
    });

    // Summary row
    const summaryRow = sheet.addRow(['Total Invoices:', filteredReportData.length, '', '', '', '', '']);
    sheet.mergeCells(`B${summaryRow.number}:H${summaryRow.number}`);
    summaryRow.getCell(1).font = { bold: true };
    summaryRow.getCell(2).font = { bold: true };
    summaryRow.eachCell((cell) => {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9D9D9' } };
        cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
    });

    // Write buffer and download
    const buffer = await workbook.xlsx.writeBuffer();
    const dateStr = new Date().toISOString().split('T')[0];
    saveBlob(buffer, `Dispatch_Report_${dateStr}.xlsx`);
}

// =============================================
// MANIFEST PREVIEW MODAL
// =============================================

function showPreviewModal() {
    if (orders.length === 0) {
        alert('No orders to print.');
        return;
    }

    // Populate preview data
    document.getElementById('preview-manifest-number').textContent = formInputs.manifestNumber.value;
    document.getElementById('preview-date').textContent = formatDate(formInputs.date.value);
    document.getElementById('preview-reg-number').textContent = formInputs.regNumber.value || 'N/A';
    document.getElementById('preview-driver').textContent = formInputs.driver.value || 'N/A';
    document.getElementById('preview-assistant').textContent = formInputs.assistant.value || 'N/A';
    document.getElementById('preview-checker').textContent = formInputs.checker.value || 'N/A';
    document.getElementById('preview-mileage').textContent = formInputs.mileage.value || '0';
    document.getElementById('preview-pallets-brown').textContent = formInputs.palletsBrown.value || '0';
    document.getElementById('preview-pallets-blue').textContent = formInputs.palletsBlue.value || '0';
    document.getElementById('preview-crates').textContent = formInputs.crates.value || '0';
    document.getElementById('preview-invoice-count').textContent = orders.length;

    // Populate orders table
    const previewOrdersList = document.getElementById('preview-orders-list');
    previewOrdersList.innerHTML = '';

    let tSku = 0, tValue = 0, tWeight = 0;

    orders.forEach(order => {
        tSku += order.sku;
        tValue += order.value;
        tWeight += order.weight;

        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${order.invoice}</td>
            <td>${order.customer}</td>
            <td>${order.area}</td>
            <td>${order.sku}</td>
            <td>$${order.value.toFixed(2)}</td>
            <td>${order.weight}</td>
        `;
        previewOrdersList.appendChild(row);
    });

    // Update totals
    document.getElementById('preview-total-sku').textContent = tSku;
    document.getElementById('preview-total-value').textContent = '$' + tValue.toFixed(2);
    document.getElementById('preview-total-weight').textContent = tWeight.toFixed(1);

    // Show modal
    const modal = document.getElementById('preview-modal');
    modal.classList.remove('hidden');
    modal.classList.add('visible');

    lucide.createIcons();
}

function hidePreviewModal() {
    const modal = document.getElementById('preview-modal');
    modal.classList.remove('visible');
    modal.classList.add('hidden');
}

function confirmAndPrint() {
    window.print();
    hidePreviewModal();
    generateExcel();
}

// Excel Generation
async function generateExcel() {
    if (orders.length === 0) {
        alert('No orders to print.');
        return;
    }

    // Validation: Ensure Truck Registration is selected
    if (!formInputs.regNumber.value) {
        alert('Please select a Truck Registration number before processing the manifest.');
        // Highlight the input
        const regInput = document.getElementById('reg-number');
        regInput.style.border = "2px solid red";
        regInput.focus();
        setTimeout(() => regInput.style.border = "", 3000);
        return;
    }

    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Manifest');

    // -- Styling & Layout based on image --

    // Set column widths to match roughly
    sheet.columns = [
        { width: 15 }, // A: Invoice
        { width: 30 }, // B: Customer Name
        { width: 15 }, // C: Customer Number (New)
        { width: 15 }, // D: Area
        { width: 8 },  // E: SKU
        { width: 10 }, // F: Value
        { width: 10 }, // G: Weight
        { width: 10 }, // H: Volume
        { width: 10 }, // I: Date
        { width: 10 }, // J: Time In
        { width: 10 }, // K: Time Out
        { width: 15 }, // L: Sign
    ];

    // ... (Skipping header blocks which use strict cell references, might need adjustment)
    // For simplicity, we'll insert the column in the data part and adjust header row 8

    // Row 8: Table Header
    const headerRow = sheet.getRow(8);
    // Added CUSTOMER ACC
    headerRow.values = ['INVOICE', 'CUSTOMER NAME', 'ACCOUNT', 'AREA', 'SKU', 'VALUE', 'WEIGHT KG', 'VOLUME', 'DATE', 'TIME IN', 'TIME OUT', 'CUSTOMER SIGN'];
    headerRow.eachCell((cell) => {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4472C4' } }; // Dark blue
        cell.font = { color: { argb: 'FFFFFFFF' }, bold: true };
        cell.alignment = { horizontal: 'center' };
        cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
    });

    // Data Rows
    orders.forEach(order => {
        const row = sheet.addRow([
            order.invoice,
            order.customer,
            order.customerNumber || 'N/A', // Added Account Column
            order.area,
            order.sku,
            '$' + order.value.toFixed(2),
            order.weight,
            order.volume,
            order.invoiceDate ? formatDate(order.invoiceDate) : '', // Populating Date column
            '', '', '' // Empty fields for Time In, Time Out, Sign
        ]);
        row.eachCell((cell) => {
            cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
        });
    });

    // Totals Row
    const lastRow = sheet.lastRow.number + 1;
    // Calculate column indices for totals (they shifted because Cust Code was removed)
    // 1=Invoice, 2=Customer, 3=Area, 4=SKU, 5=Value, 6=Weight
    const totalRow = sheet.getRow(lastRow);
    totalRow.getCell(1).value = 'Grand Total';
    totalRow.getCell(4).value = orders.reduce((sum, o) => sum + o.sku, 0); // SKU
    totalRow.getCell(5).value = '$' + orders.reduce((sum, o) => sum + o.value, 0).toFixed(2);
    totalRow.getCell(6).value = orders.reduce((sum, o) => sum + o.weight, 0);

    totalRow.eachCell((cell) => {
        cell.font = { bold: true };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9D9D9' } };
        cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
    });

    // Save report to history
    saveReport({
        date: formInputs.date.value,
        manifestNumber: formInputs.manifestNumber.value || 'UNKNOWN', // Ensure not empty
        regNumber: formInputs.regNumber.value, // FIXED: Changed from truckReg to regNumber to match API
        driver: formInputs.driver.value,
        assistant: formInputs.assistant.value,
        checker: formInputs.checker.value,
        route: '',
        totalSku: orders.reduce((sum, o) => sum + o.sku, 0),
        invoices: orders.map(o => ({
            num: o.invoice,
            orderNum: o.orderNumber, // Added Order Number
            invoiceDate: o.invoiceDate, // Added Invoice Date
            customerNumber: o.customerNumber || 'N/A', // Added Customer Number
            val: o.value,
            customer: o.customer
        })),
        referenceId: Date.now()
    });

    // Mark the current manifest number as used so the next one will be sequential
    markManifestNumberAsUsed(formInputs.manifestNumber.value);

    // Write Buffer
    const buffer = await workbook.xlsx.writeBuffer();
    const fileName = `Manifest_${formInputs.manifestNumber.value || 'Route'}.xlsx`;

    // Save locally
    saveBlob(buffer, fileName);

    // Save to server
    const blob = new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    uploadManifest(blob, fileName);

    // After printing, generate a new manifest number for the next manifest
    formInputs.manifestNumber.value = getNextManifestNumber();
    orders = [];
    setDefaultDate();
    saveState();
    renderTable();
}

function saveBlob(buffer, fileName) {
    const blob = new Blob([buffer], { type: 'application/octet-stream' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = fileName;
    link.click();
}

async function uploadManifest(blob, filename) {
    const formData = new FormData();
    formData.append('file', blob, filename);

    try {
        const response = await fetch(`${API_URL}/manifests/save`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            console.warn('Failed to save manifest to server');
        } else {
            console.log('Manifest saved to server');
        }
    } catch (error) {
        console.error('Error uploading manifest:', error);
    }
}

// Helper function to convert ArrayBuffer to Base64
function arrayBufferToBase64(buffer) {
    let binary = '';
    const bytes = new Uint8Array(buffer);
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return window.btoa(binary);
}

function formatDate(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    return `${d.getDate()}-${months[d.getMonth()]}-${d.getFullYear().toString().substr(-2)}`;
}

// =============================================
// INVOICE API & MODAL FUNCTIONS
// =============================================

async function fetchAreas() {
    // Route/Area field has been removed from the UI
    // This function is kept for API compatibility but does nothing now
}

async function fetchInvoices(area = null) {
    try {
        let url = `${API_URL}/invoices`;
        if (area) {
            url += `?area=${encodeURIComponent(area)}`;
        }

        const response = await fetch(url);
        if (!response.ok) {
            throw new Error('Failed to fetch invoices');
        }
        const data = await response.json();
        availableInvoices = data.invoices;
        return data.invoices;
    } catch (error) {
        console.error('Error fetching invoices:', error);
        alert('Could not load invoices. Make sure the API server is running (python api_server.py)');
        return [];
    }
}

function openInvoiceModal() {
    const selectedArea = null;
    document.getElementById('invoice-area-label').textContent = 'Area: All';

    const modal = document.getElementById('invoice-modal');
    modal.classList.remove('hidden');
    modal.classList.add('visible');
    loadInvoicesIntoModal(selectedArea);
}

function closeInvoiceModal() {
    const modal = document.getElementById('invoice-modal');
    modal.classList.remove('visible');
    modal.classList.add('hidden');
}

async function loadInvoicesIntoModal(area = null) {
    const tableBody = document.getElementById('invoice-select-list');
    tableBody.innerHTML = '<tr><td colspan="6" style="text-align:center;">Loading...</td></tr>';

    const invoices = await fetchInvoices(area);

    tableBody.innerHTML = '';

    if (invoices.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="6" style="text-align:center; color: #64748b;">No invoices available</td></tr>';
        return;
    }

    invoices.forEach((inv, index) => {
        const row = document.createElement('tr');
        row.dataset.index = index;
        row.innerHTML = `
            <td><input type="checkbox" class="invoice-checkbox" data-index="${index}"></td>
            <td>${inv.invoice_number || 'N/A'}</td>
            <td>${inv.order_number || 'N/A'}</td>
            <td>${inv.invoice_date || 'N/A'}</td>
            <td>${inv.customer_name}</td>
            <td>${inv.total_value}</td>
            <td>${inv.date_processed}</td>
        `;
        tableBody.appendChild(row);
    });

    // Add click listeners to checkboxes
    document.querySelectorAll('.invoice-checkbox').forEach(cb => {
        cb.addEventListener('change', updateSelectedCount);
    });

    updateSelectedCount();
    lucide.createIcons();
}

function toggleSelectAll(e) {
    const isChecked = e.target.checked;
    document.querySelectorAll('.invoice-checkbox').forEach(cb => {
        cb.checked = isChecked;
        cb.closest('tr').classList.toggle('selected', isChecked);
    });
    updateSelectedCount();
}

function updateSelectedCount() {
    const selected = document.querySelectorAll('.invoice-checkbox:checked').length;
    document.getElementById('selected-count').textContent = `${selected} selected`;

    // Update row highlighting
    document.querySelectorAll('.invoice-checkbox').forEach(cb => {
        cb.closest('tr').classList.toggle('selected', cb.checked);
    });
}

async function addSelectedInvoices() {
    const selectedCheckboxes = document.querySelectorAll('.invoice-checkbox:checked');

    if (selectedCheckboxes.length === 0) {
        alert('Please select at least one invoice.');
        return;
    }

    const filenamesToAllocate = [];

    selectedCheckboxes.forEach(cb => {
        const index = parseInt(cb.dataset.index);
        const inv = availableInvoices[index];
        filenamesToAllocate.push(inv.filename);
    });

    // Call API to add to staging
    if (filenamesToAllocate.length > 0) {
        await allocateInvoices(filenamesToAllocate);
    }

    // Reload manifest from API to get updated list
    await renderTable();

    // Uncheck all selected checkboxes after adding
    selectedCheckboxes.forEach(cb => {
        cb.checked = false;
        cb.closest('tr').classList.remove('selected');
    });

    // Reset Select All checkbox
    document.getElementById('select-all-invoices').checked = false;

    // Update the selected count display
    updateSelectedCount();

    // Enhanced feedback message
    alert(`Added ${filenamesToAllocate.length} invoice${filenamesToAllocate.length !== 1 ? 's' : ''} to manifest.`);
}


async function allocateInvoices(filenames) {
    try {
        const response = await fetch(`${API_URL}/invoices/allocate`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Username': currentUser || 'anonymous'
            },
            body: JSON.stringify({ filenames: filenames })
        });

        if (!response.ok) {
            console.warn('Could not mark invoices as allocated');
        }
    } catch (error) {
        console.warn('Allocation API call failed:', error);
    }
}

async function refreshInvoices() {
    const refreshBtn = document.getElementById('refresh-invoices-btn');
    refreshBtn.disabled = true;
    refreshBtn.innerHTML = '<i data-lucide="loader-2"></i> Scanning...';
    lucide.createIcons();

    try {
        // Trigger a rescan of PDFs on the server
        await fetch(`${API_URL}/invoices/refresh`, { method: 'POST' });

        // Reload the modal
        const selectedArea = null;
        await loadInvoicesIntoModal(selectedArea);

        // Also refresh areas in case new ones appeared
        await fetchAreas();
    } catch (error) {
        console.error('Refresh failed:', error);
    } finally {
        refreshBtn.disabled = false;
        refreshBtn.innerHTML = '<i data-lucide="refresh-cw"></i> Refresh';
        lucide.createIcons();
    }
}

// =============================================
// SETTINGS MODAL FUNCTIONS
// =============================================

// =============================================
// REWRITTEN SETTINGS MODAL LOGIC
// =============================================

async function openSettingsModal() {
    alert("Settings Clicked!"); // Debug Alert
    console.log("Opening Settings Modal...");
    const modal = document.getElementById('settings-modal');
    modal.classList.remove('hidden');
    modal.classList.add('visible');

    // 1. Fetch latest data from server
    await loadSettings();

    // 2. Render all lists
    renderSettingsList('drivers');
    renderSettingsList('assistants');
    renderSettingsList('checkers');
    renderSettingsList('routes');
    renderTrucksList();

    // 3. Populate internal dropdowns (for adding trucks)
    populateTruckFormDropdowns();

    // 4. Reset to first tab
    document.querySelector('.settings-tab[data-tab="drivers"]').click();

    lucide.createIcons();
}

function closeSettingsModal() {
    console.log("Closing Settings Modal...");
    const modal = document.getElementById('settings-modal');
    modal.classList.remove('visible');
    modal.classList.add('hidden');

    // 1. Refresh global settings data ensures main page has latest
    loadSettings().then(() => {
        // 2. Update main page dropdowns
        initTruckDropdown();
        initPersonnelDropdowns();
    });
}

function handleSettingsTabClick(e) {
    // Handle click on icon or span inside button
    const button = e.target.closest('.settings-tab');
    if (!button) return;

    const tabName = button.dataset.tab;

    // Update active tab button
    document.querySelectorAll('.settings-tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.tab === tabName);
    });

    // Update active tab content
    document.querySelectorAll('.settings-tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `tab-${tabName}`);
    });
}

function populateTruckFormDropdowns() {
    // Populate driver dropdown
    const driverSelect = document.getElementById('new-truck-driver');
    driverSelect.innerHTML = '<option value="">Default Driver (optional)</option>';
    settingsData.drivers.forEach(name => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        driverSelect.appendChild(option);
    });

    // Populate assistant dropdown
    const assistantSelect = document.getElementById('new-truck-assistant');
    assistantSelect.innerHTML = '<option value="">Default Assistant (optional)</option>';
    settingsData.assistants.forEach(name => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        assistantSelect.appendChild(option);
    });

    // Populate checker dropdown
    const checkerSelect = document.getElementById('new-truck-checker');
    checkerSelect.innerHTML = '<option value="">Default Checker (optional)</option>';
    settingsData.checkers.forEach(name => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        checkerSelect.appendChild(option);
    });
}

// =============================================
// REWRITTEN SETTINGS CRUD
// =============================================

async function addSettingsItem(category) {
    const inputMap = {
        drivers: 'new-driver-name',
        assistants: 'new-assistant-name',
        checkers: 'new-checker-name',
        routes: 'new-route-name'
    };

    const inputId = inputMap[category];
    const input = document.getElementById(inputId);
    if (!input) {
        console.error("Input not found for", category);
        return;
    }

    const value = input.value.trim();

    if (!value) {
        alert('Please enter a name.');
        return;
    }

    // Add to server
    try {
        const response = await fetch(`${API_URL}/settings`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ category, value })
        });

        if (response.ok) {
            // Update local state and UI immediately
            if (!settingsData[category]) settingsData[category] = [];
            settingsData[category].push(value);

            input.value = '';
            renderSettingsList(category);

            // If it's a person, update the Truck form dropdowns too
            if (['drivers', 'assistants', 'checkers'].includes(category)) {
                populateTruckFormDropdowns();
            }

            lucide.createIcons();
        } else {
            alert('Could not save setting. It might already exist.');
        }
    } catch (e) {
        console.error(e);
        alert('Server error saving setting: ' + e.message);
    }
}

async function removeSettingsItem(category, index) {
    const itemName = settingsData[category][index];

    if (confirm(`Are you sure you want to remove "${itemName}"?`)) {
        try {
            const response = await fetch(`${API_URL}/settings/${category}/${encodeURIComponent(itemName)}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                // Update local state
                settingsData[category].splice(index, 1);
                renderSettingsList(category);

                // Update truck dropdowns
                if (['drivers', 'assistants', 'checkers'].includes(category)) {
                    populateTruckFormDropdowns();
                }
                lucide.createIcons();
            } else {
                alert('Failed to delete setting.');
            }
        } catch (e) {
            console.error(e);
            alert('Server error deleting setting: ' + e.message);
        }
    }
}

function renderSettingsList(category) {
    const listEl = document.getElementById(`${category}-list`);
    if (!listEl) {
        console.error("List element not found for", category);
        return;
    }

    const items = settingsData[category] || [];

    if (items.length === 0) {
        listEl.innerHTML = '<div class="settings-empty">No items added yet</div>';
        return;
    }

    // Rewrite using simpler string concat
    let html = '';
    items.forEach((item, index) => {
        // Handle special chars by simple escaping if needed, but names are usually safe
        // Using onclick attributes for simplicity as requested by user's "rewrite" preference
        html += `
        <div class="settings-item">
            <div class="settings-item-info">
                <span class="settings-item-name">${item}</span>
            </div>
            <div class="settings-item-actions">
                <button class="btn-icon btn-delete" onclick="removeSettingsItem('${category}', ${index})" title="Delete">
                    <i data-lucide="trash-2"></i>
                </button>
            </div>
        </div>
        `;
    });

    listEl.innerHTML = html;
}

// Editing functionality removed for simplicity as per rewrite request.
// To re-enable, implement specific edit modals or inline editing here.

async function addTruck() {
    const regInput = document.getElementById('new-truck-reg');
    const driverSelect = document.getElementById('new-truck-driver');
    const assistantSelect = document.getElementById('new-truck-assistant');
    const checkerSelect = document.getElementById('new-truck-checker');

    const reg = regInput.value.trim().toUpperCase();

    if (!reg) {
        alert('Please enter a registration number.');
        return;
    }

    const newTruck = {
        reg: reg,
        driver: driverSelect.value || '',
        assistant: assistantSelect.value || '',
        checker: checkerSelect.value || ''
    };

    try {
        const response = await fetch(`${API_URL}/trucks`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(newTruck)
        });

        if (response.ok) {
            settingsData.trucks.push(newTruck);

            // Clear form
            regInput.value = '';
            driverSelect.value = '';
            assistantSelect.value = '';
            checkerSelect.value = '';

            renderTrucksList();
            lucide.createIcons();
        } else {
            alert('Could not save truck. Registration might exist.');
        }
    } catch (e) {
        console.error(e);
        alert('Server error saving truck.');
    }
}

async function removeTruck(index) {
    const truck = settingsData.trucks[index];

    if (confirm(`Are you sure you want to remove truck "${truck.reg}"?`)) {
        try {
            const response = await fetch(`${API_URL}/trucks/${encodeURIComponent(truck.reg)}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                settingsData.trucks.splice(index, 1);
                renderTrucksList();
                lucide.createIcons();
            } else {
                alert('Failed to delete truck.');
            }
        } catch (e) {
            console.error(e);
            alert('Server error deleting truck.');
        }
    }
}

function renderTrucksList() {
    const listEl = document.getElementById('trucks-list');

    if (settingsData.trucks.length === 0) {
        listEl.innerHTML = '<div class="settings-empty">No trucks added yet</div>';
        return;
    }

    listEl.innerHTML = settingsData.trucks.map((truck, index) => {
        const details = [];
        if (truck.driver) details.push(`Driver: ${truck.driver}`);
        if (truck.assistant) details.push(`Assistant: ${truck.assistant}`);
        if (truck.checker) details.push(`Checker: ${truck.checker}`);

        return `
            <div class="settings-item" id="settings-truck-${index}">
                <div class="settings-item-info">
                    <span class="settings-item-name">${truck.reg}</span>
                    ${details.length > 0 ? `<span class="settings-item-details">${details.join(' | ')}</span>` : ''}
                </div>
                <div class="settings-item-actions">
                    <button class="btn-icon btn-edit" onclick="editTruck(${index})" title="Edit">
                        <i data-lucide="pencil"></i>
                    </button>
                    <button class="btn-icon btn-delete" onclick="removeTruck(${index})" title="Delete">
                        <i data-lucide="trash-2"></i>
                    </button>
                </div>
            </div>
        `;
    }).join('');

    lucide.createIcons();
}

function editTruck(index) {
    const truck = settingsData.trucks[index];
    const itemEl = document.getElementById(`settings-truck-${index}`);

    // Build driver options
    const driverOptions = settingsData.drivers.map(d =>
        `<option value="${d}" ${d === truck.driver ? 'selected' : ''}>${d}</option>`
    ).join('');

    // Build assistant options
    const assistantOptions = settingsData.assistants.map(a =>
        `<option value="${a}" ${a === truck.assistant ? 'selected' : ''}>${a}</option>`
    ).join('');

    // Build checker options
    const checkerOptions = settingsData.checkers.map(c =>
        `<option value="${c}" ${c === truck.checker ? 'selected' : ''}>${c}</option>`
    ).join('');

    // Replace the item with an edit form
    itemEl.innerHTML = `
        <div class="settings-edit-form truck-edit-form">
            <div class="truck-edit-grid">
                <div class="form-group">
                    <label>Registration #</label>
                    <input type="text" id="edit-truck-reg-${index}" value="${truck.reg}">
                </div>
                <div class="form-group">
                    <label>Driver</label>
                    <select id="edit-truck-driver-${index}">
                        <option value="">No default driver</option>
                        ${driverOptions}
                    </select>
                </div>
                <div class="form-group">
                    <label>Assistant</label>
                    <select id="edit-truck-assistant-${index}">
                        <option value="">No default assistant</option>
                        ${assistantOptions}
                    </select>
                </div>
                <div class="form-group">
                    <label>Checker</label>
                    <select id="edit-truck-checker-${index}">
                        <option value="">No default checker</option>
                        ${checkerOptions}
                    </select>
                </div>
            </div>
            <div class="settings-edit-actions">
                <button class="btn btn-primary btn-sm" onclick="saveTruckEdit(${index})">
                    <i data-lucide="check"></i> Save
                </button>
                <button class="btn btn-secondary btn-sm" onclick="cancelTruckEdit()">
                    <i data-lucide="x"></i> Cancel
                </button>
            </div>
        </div>
    `;

    lucide.createIcons();
}

async function saveTruckEdit(index) {
    const regInput = document.getElementById(`edit-truck-reg-${index}`);
    const driverSelect = document.getElementById(`edit-truck-driver-${index}`);
    const assistantSelect = document.getElementById(`edit-truck-assistant-${index}`);
    const checkerSelect = document.getElementById(`edit-truck-checker-${index}`);

    const newReg = regInput.value.trim().toUpperCase();

    if (!newReg) {
        alert('Please enter a registration number.');
        return;
    }

    const updatedTruck = {
        reg: newReg,
        driver: driverSelect.value || '',
        assistant: assistantSelect.value || '',
        checker: checkerSelect.value || ''
    };

    try {
        const response = await fetch(`${API_URL}/trucks/${encodeURIComponent(settingsData.trucks[index].reg)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(updatedTruck)
        });

        if (response.ok) {
            settingsData.trucks[index] = updatedTruck;
            renderTrucksList();
        } else {
            alert('Failed to update truck.');
        }
    } catch (e) {
        console.error(e);
        alert('Server error updating truck.');
    }
}

function cancelTruckEdit() {
    renderTrucksList();
}

// =============================================
// CUSTOMER-ROUTE MANAGEMENT FUNCTIONS
// =============================================

// Get the route assigned to a customer name (supports partial matching)
function getRouteForCustomer(customerName) {
    if (!customerName || !settingsData.customerRoutes) return null;
    // Case-insensitive lookup with partial matching
    const normalizedName = customerName.trim().toUpperCase();

    // First try exact match
    for (const [customer, route] of Object.entries(settingsData.customerRoutes)) {
        if (customer.toUpperCase() === normalizedName) {
            return route;
        }
    }

    // Then try partial match (customer name contains the pattern)
    // Sort by pattern length descending to match longest patterns first
    const sortedEntries = Object.entries(settingsData.customerRoutes)
        .sort((a, b) => b[0].length - a[0].length);

    for (const [customer, route] of sortedEntries) {
        const pattern = customer.toUpperCase();
        // Check if the invoice customer name contains the assigned pattern
        if (normalizedName.includes(pattern)) {
            return route;
        }
    }

    return null;
}

// Populate route dropdown for customer-route assignment
function populateCustomerRouteDropdown() {
    const select = document.getElementById('new-customer-route');
    if (!select) return;

    select.innerHTML = '<option value="">Select Route</option>';
    settingsData.routes.forEach(route => {
        const option = document.createElement('option');
        option.value = route;
        option.textContent = route;
        select.appendChild(option);
    });
}

// Populate route filter dropdowns in modals
function populateRouteFilterDropdowns() {
    // Invoice modal route filter
    const invoiceFilter = document.getElementById('invoice-route-filter');
    if (invoiceFilter) {
        invoiceFilter.innerHTML = '<option value="">All Routes</option>';
        settingsData.routes.forEach(route => {
            const option = document.createElement('option');
            option.value = route;
            option.textContent = route;
            invoiceFilter.appendChild(option);
        });
    }

    // Report modal route filter
    const reportFilter = document.getElementById('report-route-filter');
    if (reportFilter) {
        reportFilter.innerHTML = '<option value="">All Routes</option>';
        settingsData.routes.forEach(route => {
            const option = document.createElement('option');
            option.value = route;
            option.textContent = route;
            reportFilter.appendChild(option);
        });
    }
}

// Add a customer-route assignment
async function addCustomerRoute() {
    const customerInput = document.getElementById('new-customer-name');
    const routeSelect = document.getElementById('new-customer-route');

    const customerName = customerInput.value.trim();
    const routeName = routeSelect.value;

    if (!customerName) {
        alert('Please enter a customer name.');
        return;
    }

    if (!routeName) {
        alert('Please select a route.');
        return;
    }

    // Check for existing assignment (case-insensitive)
    const normalizedNewName = customerName.toUpperCase();
    const existingName = Object.keys(settingsData.customerRoutes).find(c => c.toUpperCase() === normalizedNewName);

    if (existingName) {
        const existingRoute = settingsData.customerRoutes[existingName];
        // User requested: "if has already been allocated then dont allocate it again or rather tell me"
        // A confirmation dialog "tells them" and allows choice.
        if (!confirm(`Customer "${existingName}" is already assigned to route "${existingRoute}".\n\nDo you want to update this assignment?`)) {
            return;
        }
    }

    // Attempt to save to server
    try {
        const response = await fetch(`${API_URL}/customer-routes`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                customer_name: customerName,
                route_name: routeName
            })
        });

        if (response.ok) {
            settingsData.customerRoutes[customerName] = routeName;

            customerInput.value = '';
            routeSelect.value = '';

            renderCustomerRoutesList();
            lucide.createIcons();
        } else {
            alert('Failed to save customer route assignment.');
        }
    } catch (e) {
        console.error(e);
        alert('Server error saving assignment.');
    }
}

// Remove a customer-route assignment
async function removeCustomerRoute(customerName) {
    if (confirm(`Remove route assignment for "${customerName}"?`)) {
        try {
            const response = await fetch(`${API_URL}/customer-routes/${encodeURIComponent(customerName)}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                delete settingsData.customerRoutes[customerName];
                renderCustomerRoutesList();
                lucide.createIcons();
            } else {
                alert('Failed to delete assignment.');
            }
        } catch (e) {
            console.error(e);
            alert('Server error deleting assignment.');
        }
    }
}

// Render customer-route assignments list grouped by route
function renderCustomerRoutesList() {
    const listEl = document.getElementById('customer-routes-list');
    if (!listEl) return;

    const assignments = Object.entries(settingsData.customerRoutes || {});

    if (assignments.length === 0) {
        listEl.innerHTML = '<div class="settings-empty">No customer-route assignments yet</div>';
        return;
    }

    // Group by route
    const groupedByRoute = {};
    assignments.forEach(([customer, route]) => {
        if (!groupedByRoute[route]) {
            groupedByRoute[route] = [];
        }
        groupedByRoute[route].push(customer);
    });

    // Build HTML
    let html = '';
    for (const [route, customers] of Object.entries(groupedByRoute)) {
        html += `<div class="settings-item-group">
            <div class="settings-item-group-header">
                <span class="route-badge">${route}</span>
                <span class="customer-count">${customers.length} customer${customers.length > 1 ? 's' : ''}</span>
            </div>`;

        customers.forEach(customer => {
            const escapedCustomer = customer.replace(/'/g, "\\'").replace(/"/g, "&quot;");
            html += `
            <div class="settings-item" id="customer-route-item-${escapedCustomer.replace(/\s/g, '-')}">
                <div class="settings-item-info">
                    <span class="settings-item-name">${customer}</span>
                </div>
                <div class="settings-item-actions">
                    <button class="btn-icon btn-edit" onclick="editCustomerRoute('${escapedCustomer}', '${route}')" title="Edit">
                        <i data-lucide="pencil"></i>
                    </button>
                    <button class="btn-icon btn-delete" onclick="removeCustomerRoute('${escapedCustomer}')" title="Remove">
                        <i data-lucide="trash-2"></i>
                    </button>
                </div>
            </div>`;
        });

        html += '</div>';
    }

    listEl.innerHTML = html;
    lucide.createIcons();
}

// Edit a customer-route assignment
function editCustomerRoute(customerName, currentRoute) {
    const escapedId = customerName.replace(/'/g, "\\'").replace(/"/g, "&quot;").replace(/\s/g, '-');
    const itemEl = document.getElementById(`customer-route-item-${escapedId}`);
    if (!itemEl) {
        // Fallback: find by searching
        const items = document.querySelectorAll('.settings-item');
        for (const item of items) {
            const nameSpan = item.querySelector('.settings-item-name');
            if (nameSpan && nameSpan.textContent === customerName) {
                showCustomerRouteEditForm(item, customerName, currentRoute);
                return;
            }
        }
        return;
    }
    showCustomerRouteEditForm(itemEl, customerName, currentRoute);
}

function showCustomerRouteEditForm(itemEl, customerName, currentRoute) {
    // Build route options
    const routeOptions = settingsData.routes.map(r =>
        `<option value="${r}" ${r === currentRoute ? 'selected' : ''}>${r}</option>`
    ).join('');

    itemEl.innerHTML = `
        <div class="settings-edit-form customer-route-edit">
            <input type="text" class="settings-edit-input" id="edit-customer-name" value="${customerName}">
            <select id="edit-customer-route">
                ${routeOptions}
            </select>
            <div class="settings-edit-actions">
                <button class="btn btn-primary btn-sm" onclick="saveCustomerRouteEdit('${customerName.replace(/'/g, "\\'")}')">
                    <i data-lucide="check"></i> Save
                </button>
                <button class="btn btn-secondary btn-sm" onclick="renderCustomerRoutesList()">
                    <i data-lucide="x"></i> Cancel
                </button>
            </div>
        </div>
    `;

    // Focus input and select all
    const input = document.getElementById('edit-customer-name');
    input.focus();
    input.select();

    // Allow Enter to save, Escape to cancel
    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            saveCustomerRouteEdit(customerName);
        } else if (e.key === 'Escape') {
            renderCustomerRoutesList();
        }
    });

    lucide.createIcons();
}

function saveCustomerRouteEdit(originalCustomerName) {
    const newCustomerName = document.getElementById('edit-customer-name').value.trim();
    const newRoute = document.getElementById('edit-customer-route').value;

    if (!newCustomerName) {
        alert('Please enter a customer name.');
        return;
    }

    if (!newRoute) {
        alert('Please select a route.');
        return;
    }

    // Remove old entry
    delete settingsData.customerRoutes[originalCustomerName];

    // Add new entry
    settingsData.customerRoutes[newCustomerName] = newRoute;

    saveSettings();
    renderCustomerRoutesList();
}

// =============================================
// UPDATED INVOICE FILTERING WITH ROUTE
// =============================================

async function loadInvoicesIntoModalFiltered(area = null, routeFilter = null) {
    const tableBody = document.getElementById('invoice-select-list');
    tableBody.innerHTML = '<tr><td colspan="7" style="text-align:center;">Loading...</td></tr>';

    // Fetch latest data (populates availableInvoices)
    await fetchInvoices(area);

    // Render with filters
    renderFilteredInvoices();
}

// Render invoices applying both Route and Search filters locally
function renderFilteredInvoices() {
    const tableBody = document.getElementById('invoice-select-list');
    const routeFilter = document.getElementById('invoice-route-filter').value;
    const searchInput = document.getElementById('invoice-search');
    const searchTerm = searchInput ? searchInput.value.trim().toLowerCase() : '';

    const clearBtn = document.getElementById('clear-invoice-search-btn');
    if (clearBtn) {
        if (searchTerm) {
            clearBtn.classList.remove('hidden');
        } else {
            clearBtn.classList.add('hidden');
        }
    }

    tableBody.innerHTML = '';

    let filteredInvoices = availableInvoices;

    // Apply Route Filter
    if (routeFilter) {
        filteredInvoices = filteredInvoices.filter(inv => {
            const customerRoute = getRouteForCustomer(inv.customer_name);
            return customerRoute && customerRoute.toUpperCase() === routeFilter.toUpperCase();
        });
    }

    // Apply Search Filter
    if (searchTerm) {
        filteredInvoices = filteredInvoices.filter(inv => {
            const invNum = (inv.invoice_number || '').toLowerCase();
            const orderNum = (inv.order_number || '').toLowerCase();
            const custName = (inv.customer_name || '').toLowerCase();
            return invNum.includes(searchTerm) || orderNum.includes(searchTerm) || custName.includes(searchTerm);
        });
    }

    if (filteredInvoices.length === 0) {
        let message = 'No invoices available';
        if (routeFilter || searchTerm) {
            message = 'No invoices match your filters';
        }
        tableBody.innerHTML = `<tr><td colspan="7" style="text-align:center; color: #64748b;">${message}</td></tr>`;
        return;
    }

    filteredInvoices.forEach((inv) => {
        // Use original index from availableInvoices to ensure correct selection binding
        const originalIndex = availableInvoices.indexOf(inv);

        const row = document.createElement('tr');
        row.dataset.index = originalIndex;
        row.innerHTML = `
            <td><input type="checkbox" class="invoice-checkbox" data-index="${originalIndex}"></td>
            <td>${inv.invoice_number || 'N/A'}</td>
            <td>${inv.order_number || 'N/A'}</td>
            <td>${inv.invoice_date || 'N/A'}</td>
            <td>${inv.customer_name}</td>
            <td>${inv.total_value}</td>
            <td>${inv.date_processed}</td>
        `;
        tableBody.appendChild(row);
    });

    // Add click listeners to checkboxes
    document.querySelectorAll('.invoice-checkbox').forEach(cb => {
        cb.addEventListener('change', updateSelectedCount);
    });

    updateSelectedCount();
    lucide.createIcons();
}

// Handle route filter change in invoice modal
function handleInvoiceRouteFilterChange() {
    renderFilteredInvoices();
}

function handleInvoiceSearch() {
    renderFilteredInvoices();
}

function clearInvoiceSearch() {
    const input = document.getElementById('invoice-search');
    if (input) {
        input.value = '';
        handleInvoiceSearch();
        input.focus();
    }
}

// =============================================
// UPDATED REPORT FILTERING WITH ROUTE
// =============================================

// Modified renderReports to include route filtering
// Modified renderReports to include route filtering AND FETCHING from API
async function renderReportsWithRouteFilter() {
    const list = document.getElementById('reports-list');
    list.innerHTML = '<tr><td colspan="11" style="text-align:center; padding: 2rem;">Loading reports...</td></tr>';

    // Ensure Manifest header is visible
    const manifestHeader = document.querySelector('#reports-table th:nth-child(3)');
    if (manifestHeader) manifestHeader.style.display = '';

    const dateFrom = document.getElementById('report-date-from').value;
    const dateTo = document.getElementById('report-date-to').value;
    const routeFilter = document.getElementById('report-route-filter').value;

    // FETCH DATA FROM API - Use /reports/dispatched for consistency with renderReports()
    try {
        const params = new URLSearchParams();
        if (dateFrom) params.append('date_from', dateFrom);
        if (dateTo) params.append('date_to', dateTo);

        const response = await fetch(`${API_URL}/reports/dispatched?${params.toString()}`);
        if (!response.ok) throw new Error("Failed to fetch reports");

        const data = await response.json();

        // Transform invoice-level data (already flattened by API)
        filteredReportData = (data.invoices || []).map(inv => ({
            invoice: inv.invoice_number || 'N/A',
            orderNumber: inv.order_number || 'N/A',
            manifest: inv.manifest_number || 'N/A',
            truckReg: inv.reg_number || 'N/A',
            customer: inv.customer_name || 'N/A',
            customerNumber: inv.customer_number || 'N/A',
            invoiceDate: inv.invoice_date || 'N/A',
            dateDispatched: inv.date_dispatched || 'N/A', // CORRECT: uses dispatch timestamp
            driver: inv.driver || 'N/A',
            assistant: inv.assistant || 'N/A',
            checker: inv.checker || 'N/A'
        }));

        // Apply route filter if specified
        if (routeFilter) {
            filteredReportData = filteredReportData.filter(inv => {
                const customerRoute = getRouteForCustomer(inv.customer);
                // Match logic: customer MUST have a route and it MUST match
                return customerRoute && customerRoute.toUpperCase() === routeFilter.toUpperCase();
            });
        }
    } catch (e) {
        console.error("Error fetching reports:", e);
        list.innerHTML = '<tr><td colspan="11" style="text-align:center; color: red;">Error loading reports.</td></tr>';
        return;
    }

    // Sort by date (newest first)
    filteredReportData.sort((a, b) => new Date(b.dateDispatched) - new Date(a.dateDispatched));

    // Apply search filter
    const searchTerm = document.getElementById('report-search').value.trim().toLowerCase();
    let displayData = filteredReportData;

    if (searchTerm) {
        displayData = filteredReportData.filter(row => {
            const invoiceMatch = row.invoice.toLowerCase().includes(searchTerm);
            const orderMatch = row.orderNumber.toLowerCase().includes(searchTerm);
            const customerMatch = row.customer.toLowerCase().includes(searchTerm);
            const manifestMatch = (row.manifest || '').toLowerCase().includes(searchTerm);
            return invoiceMatch || orderMatch || customerMatch || manifestMatch;
        });
    }

    list.innerHTML = '';

    // Render rows
    if (displayData.length === 0) {
        const message = searchTerm
            ? 'No invoices match your search criteria.'
            : routeFilter
                ? `No dispatched orders found for route "${routeFilter}".`
                : 'No dispatched orders found for the selected date range.';
        list.innerHTML = `<tr><td colspan="11" style="text-align:center; color: #64748b; padding: 2rem;">${message}</td></tr>`;
    } else {
        displayData.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong>${row.invoice}</strong></td>
                <td>${row.orderNumber}</td>
                <td>${row.manifest}</td>
                <td>${row.truckReg}</td>
                <td>${row.customer}</td>
                <td>${row.customerNumber || 'N/A'}</td>
                <td>${row.invoiceDate}</td>
                <td>${formatDate(row.dateDispatched)}</td>
                <td>${row.driver}</td>
                <td>${row.assistant}</td>
                <td>${row.checker}</td>
            `;
            list.appendChild(tr);
        });
    }

    // Update summary
    const summaryEl = document.getElementById('report-summary');
    const countEl = document.getElementById('report-count');

    if (filteredReportData.length > 0 && (dateFrom || dateTo)) {
        summaryEl.classList.remove('hidden');
        const fromText = dateFrom ? formatDate(dateFrom) : 'Beginning';
        const toText = dateTo ? formatDate(dateTo) : 'Today';
        const routeText = routeFilter ? ` | Route: ${routeFilter}` : '';
        summaryEl.innerHTML = `
            <span class="report-summary-text">Dispatch Report: ${fromText} to ${toText}${routeText}</span>
            <span class="report-summary-count">${filteredReportData.length} invoices</span>
        `;
    } else {
        summaryEl.classList.add('hidden');
    }

    currentPrintableData = displayData;
    countEl.textContent = `${displayData.length} invoices found`;

    lucide.createIcons();
}

// Override original renderReports
const originalRenderReports = renderReports;
renderReports = renderReportsWithRouteFilter;

// Override original filter function
const originalFilterReports = filterReports;
filterReports = async function () {
    const btn = document.getElementById('filter-btn');
    const originalContent = btn.innerHTML;

    try {
        // Set loading state
        btn.disabled = true;
        btn.innerHTML = '<i data-lucide="loader-2" class="spin"></i> Generating...';
        lucide.createIcons();

        // Allow UI to update
        await new Promise(resolve => setTimeout(resolve, 50));

        if (currentReportView === 'outstanding') {
            await renderOutstandingOrders();
        } else {
            renderReportsWithRouteFilter();
        }
    } catch (error) {
        console.error('Error generating report:', error);
    } finally {
        // Restore button state
        btn.disabled = false;
        btn.innerHTML = originalContent;
        lucide.createIcons();
    }
};

// =============================================
// UPDATED SETTINGS MODAL FUNCTIONS
// =============================================

// Override openSettingsModal to include customer routes
const originalOpenSettingsModal = openSettingsModal;
openSettingsModal = function () {
    const modal = document.getElementById('settings-modal');
    modal.classList.remove('hidden');
    modal.classList.add('visible');

    // Populate the truck form dropdowns with current personnel
    populateTruckFormDropdowns();

    // Populate customer route dropdown
    populateCustomerRouteDropdown();

    // Populate customer suggestions datalist
    populateCustomerSuggestions();

    // Render all settings lists
    renderSettingsList('drivers');
    renderSettingsList('assistants');
    renderSettingsList('checkers');
    renderSettingsList('routes');
    renderCustomerRoutesList();
    renderTrucksList();

    lucide.createIcons();
};

async function populateCustomerSuggestions() {
    const dataList = document.getElementById('customer-suggestions');
    if (!dataList) return;

    dataList.innerHTML = '';

    // Collect unique customers
    const customers = new Set();

    try {
        // Fetch ALL customers from database (pending and allocated)
        const response = await fetch(`${API_URL}/customers`);
        if (response.ok) {
            const data = await response.json();
            data.customers.forEach(name => customers.add(name));
        }
    } catch (error) {
        console.warn('Could not fetch customers from API:', error);
    }

    // Add from existing customer routes (in case some aren't in DB yet)
    if (settingsData && settingsData.customerRoutes) {
        Object.keys(settingsData.customerRoutes).forEach(name => {
            customers.add(name);
        });
    }

    // Convert to sorted array
    const sortedCustomers = Array.from(customers).sort();

    sortedCustomers.forEach(customer => {
        const option = document.createElement('option');
        option.value = customer;
        dataList.appendChild(option);
    });
}

// =============================================
// ADDITIONAL EVENT LISTENERS
// =============================================

// Add event listeners for new functionality when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    // Customer route button
    const addCustomerRouteBtn = document.getElementById('add-customer-route-btn');
    if (addCustomerRouteBtn) {
        addCustomerRouteBtn.addEventListener('click', addCustomerRoute);
    }

    // Invoice route filter
    const invoiceRouteFilter = document.getElementById('invoice-route-filter');
    if (invoiceRouteFilter) {
        invoiceRouteFilter.addEventListener('change', handleInvoiceRouteFilterChange);
    }

    // Report route filter (triggers on Generate Report button click)
    // Already handled by existing filter-btn click handler

    // Invoice search filter
    const invoiceSearch = document.getElementById('invoice-search');
    if (invoiceSearch) {
        invoiceSearch.addEventListener('input', handleInvoiceSearch);
    }

    const clearInvoiceSearchBtn = document.getElementById('clear-invoice-search-btn');
    if (clearInvoiceSearchBtn) {
        clearInvoiceSearchBtn.addEventListener('click', clearInvoiceSearch);
    }

    // Populate route filter dropdowns on load
    populateRouteFilterDropdowns();
});

// Update openInvoiceModal to populate route filter and use filtered function
const originalOpenInvoiceModal = openInvoiceModal;
openInvoiceModal = function () {
    const selectedArea = null;
    document.getElementById('invoice-area-label').textContent = 'Area: All';

    // Populate route filter dropdown
    populateRouteFilterDropdowns();

    // Reset route filter to "All Routes"
    const routeFilter = document.getElementById('invoice-route-filter');
    if (routeFilter) {
        routeFilter.value = '';
    }

    const modal = document.getElementById('invoice-modal');
    modal.classList.remove('hidden');
    modal.classList.add('visible');
    loadInvoicesIntoModalFiltered(selectedArea, null);
};

// Update showReports to populate route filter
const originalShowReports = showReports;
showReports = function () {
    const modal = document.getElementById('reports-modal');
    modal.classList.remove('hidden');
    modal.classList.add('visible');

    // Set default date range to current month
    const today = new Date();
    const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);

    document.getElementById('report-date-from').valueAsDate = firstDayOfMonth;
    document.getElementById('report-date-to').valueAsDate = today;

    // Populate route filter dropdown
    populateRouteFilterDropdowns();

    // Reset route filter
    const routeFilter = document.getElementById('report-route-filter');
    if (routeFilter) {
        routeFilter.value = '';
    }

    renderReportsWithRouteFilter();
};

// Global state to track current view
let currentPrintableData = [];

function switchReportView(view) {
    currentReportView = view;

    // Update button states
    const dispatchedBtn = document.getElementById('view-dispatched-btn');
    const outstandingBtn = document.getElementById('view-outstanding-btn');

    if (view === 'dispatched') {
        dispatchedBtn.classList.add('active');
        outstandingBtn.classList.remove('active');
        renderReports();
    } else {
        dispatchedBtn.classList.remove('active');
        outstandingBtn.classList.add('active');
        renderOutstandingOrders();
    }
}

async function renderOutstandingOrders() {
    const list = document.getElementById('reports-list');
    const summaryEl = document.getElementById('report-summary');
    const countEl = document.getElementById('report-count');

    // Hide Manifest header
    const manifestHeader = document.querySelector('#reports-table th:nth-child(3)');
    if (manifestHeader) manifestHeader.style.display = 'none';

    list.innerHTML = '<tr><td colspan="11" style="text-align:center;">Loading outstanding orders...</td></tr>';
    summaryEl.classList.add('hidden');

    try {
        // Fetch pending invoices from API
        const response = await fetch(`${API_URL}/invoices`);
        if (!response.ok) throw new Error('Failed to fetch outstanding orders');

        const data = await response.json();
        let outstandingInvoices = data.invoices || [];

        // Apply route filter
        const routeFilter = document.getElementById('report-route-filter').value;
        if (routeFilter) {
            outstandingInvoices = outstandingInvoices.filter(inv => {
                const customerRoute = getRouteForCustomer(inv.customer_name);
                return customerRoute && customerRoute.toUpperCase() === routeFilter.toUpperCase();
            });
        }

        // Apply search filter
        const searchTerm = document.getElementById('report-search').value.trim().toLowerCase();
        if (searchTerm) {
            outstandingInvoices = outstandingInvoices.filter(inv => {
                const invNum = (inv.invoice_number || '').toLowerCase();
                const orderNum = (inv.order_number || '').toLowerCase();
                const custName = (inv.customer_name || '').toLowerCase();
                return invNum.includes(searchTerm) || orderNum.includes(searchTerm) || custName.includes(searchTerm);
            });
        }

        // Save for printing
        currentPrintableData = outstandingInvoices.map(inv => ({
            invoice: inv.invoice_number || 'N/A',
            orderNumber: inv.order_number || 'N/A',
            manifest: 'Pending',
            truckReg: '-',
            customer: inv.customer_name || 'N/A',
            invoiceDate: inv.invoice_date || 'N/A',
            dateDispatched: 'Pending',
            driver: '-',
            assistant: '-',
            checker: '-'
        }));

        list.innerHTML = '';

        if (outstandingInvoices.length === 0) {
            const message = searchTerm || routeFilter
                ? 'No outstanding orders match your filters.'
                : 'No outstanding orders found. All invoices have been dispatched!';
            list.innerHTML = `<tr><td colspan="11" style="text-align:center; color: #64748b; padding: 2rem;">${message}</td></tr>`;
        } else {
            outstandingInvoices.forEach(inv => {
                const tr = document.createElement('tr');
                tr.style.backgroundColor = '#fff7ed'; // Light orange background for outstanding
                tr.innerHTML = `
                    <td><strong>${inv.invoice_number || 'N/A'}</strong></td>
                    <td>${inv.order_number || 'N/A'}</td>
                    <td>-</td>
                    <td>${inv.customer_name || 'N/A'}</td>
                    <td>${inv.customer_number || 'N/A'}</td>
                    <td>${inv.invoice_date || 'N/A'}</td>
                    <td style="color: #f59e0b; font-weight: 600;">Pending</td>
                    <td>-</td>
                    <td>-</td>
                    <td>-</td>
                `;
                list.appendChild(tr);
            });
        }

        countEl.textContent = `${outstandingInvoices.length} outstanding orders`;

        // Update summary
        if (outstandingInvoices.length > 0) {
            summaryEl.classList.remove('hidden');
            const routeText = routeFilter ? ` | Route: ${routeFilter}` : '';
            summaryEl.innerHTML = `
                <span class="report-summary-text">Outstanding Orders${routeText}</span>
                <span class="report-summary-count">${outstandingInvoices.length} pending</span>
            `;
        }

    } catch (error) {
        console.error('Error fetching outstanding orders:', error);
        list.innerHTML = '<tr><td colspan="11" style="text-align:center; color: red;">Error loading outstanding orders. Please try again.</td></tr>';
    }

    lucide.createIcons();
}

// --- RBAC & Landing Page Functions ---

function showLandingPage() {
    const landing = document.getElementById('landing-overlay');
    if (landing) {
        landing.classList.remove('hidden');
        landing.style.opacity = '1';
        landing.style.pointerEvents = 'auto';
    }
}

function hideLandingPage() {
    const landing = document.getElementById('landing-overlay');
    if (landing) {
        landing.classList.add('hidden');
        // Remove inline styles that override hidden class
        landing.style.opacity = '';
        landing.style.pointerEvents = '';
        landing.style.display = 'none'; // Force hide
    }
}

function openLoginModal(intent) {
    loginIntent = intent;
    const modal = document.getElementById('login-modal');
    modal.classList.remove('hidden');
    modal.classList.add('visible');

    // Reset fields
    document.getElementById('login-username').value = '';
    document.getElementById('login-password').value = '';
    document.getElementById('login-error').classList.add('hidden');
    document.getElementById('access-error').classList.add('hidden');

    setTimeout(() => document.getElementById('login-username').focus(), 100);
}

async function handleLogin() {
    const usernameEl = document.getElementById('login-username');
    const passwordEl = document.getElementById('login-password');
    const loginBtn = document.getElementById('login-submit-btn');
    const loginError = document.getElementById('login-error');
    const accessError = document.getElementById('access-error');

    const username = usernameEl.value.trim();
    const password = passwordEl.value;

    // Validate inputs before sending
    if (!username || !password) {
        loginError.textContent = 'Please enter both username and password.';
        loginError.classList.remove('hidden');
        accessError.classList.add('hidden');
        return;
    }

    // Reset error messages
    loginError.classList.add('hidden');
    accessError.classList.add('hidden');

    // Show loading state on button
    loginBtn.disabled = true;
    loginBtn.innerHTML = '<i data-lucide="loader-2" class="spin"></i> Logging in...';
    lucide.createIcons();

    console.log(`[Login] Attempting login for user: "${username}"`);

    try {
        const response = await fetch('/auth/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });

        console.log(`[Login] Server responded with status: ${response.status}`);

        if (response.status === 401) {
            // Invalid credentials
            console.warn('[Login] Invalid credentials.');
            loginError.textContent = 'Incorrect username or password.';
            loginError.classList.remove('hidden');
            passwordEl.value = '';
            passwordEl.focus();
            return;
        }

        if (!response.ok) {
            // Other server errors (500, 503 etc.)
            const detail = await response.text();
            console.error(`[Login] Server error ${response.status}:`, detail);
            loginError.textContent = `Server error (${response.status}). Please try again later.`;
            loginError.classList.remove('hidden');
            return;
        }

        const data = await response.json();
        console.log('[Login] Login successful. User data received:', data.user);

        const user = {
            username: data.user.username,
            isAdmin: data.user.isAdmin,
            canManifest: data.user.canManifest
        };

        // Check permissions based on what the user intends to do
        if (loginIntent === 'manifest') {
            if (user.canManifest || user.isAdmin) {
                console.log('[Login] Manifest access granted.');
                finalizeLogin(user);
            } else {
                console.warn('[Login] User lacks manifest permission.');
                accessError.textContent = 'You do not have permission to access the Manifest system.';
                accessError.classList.remove('hidden');
                loginError.classList.add('hidden');
            }
        } else {
            // Report intent - all authenticated users can view reports
            console.log('[Login] Report access granted.');
            finalizeLogin(user);
        }

    } catch (error) {
        // Network-level failure (no connection, DNS failure, CORS etc.)
        console.error('[Login] Network error during login:', error);
        loginError.textContent = 'Could not connect to the server. Check your network connection.';
        loginError.classList.remove('hidden');
    } finally {
        loginBtn.disabled = false;
        loginBtn.textContent = 'Login';
    }
}

async function finalizeLogin(user) {
    currentUser = user.username;  // FIX: Store username string, not object
    // Derive role from user object
    if (user.isAdmin) {
        currentUserRole = 'admin';
    } else {
        currentUserRole = 'user'; // default
    }


    // Explicitly re-initialize the main form data
    // This handles cases where page loaded before server was ready
    await loadSettings();
    initManifestNumber();
    setDefaultDate();

    // If manifest number is STILL missing, force a reset/generate
    if (!formInputs.manifestNumber.value) {
        console.log("Manifest number missing, regenerating...");
        formInputs.manifestNumber.value = getNextManifestNumber();
        saveState();
    }


    // Hide Modal & Landing
    document.getElementById('login-modal').classList.remove('visible');
    document.getElementById('login-modal').classList.add('hidden');
    hideLandingPage();

    // Apply UI Permissions
    applyPermissions();

    // Navigate if needed
    if (loginIntent === 'report') {
        showReports();
    }
}

function applyPermissions() {
    // currentUser is now just a string (username), not an object
    // Use currentUserRole instead
    if (!currentUser) return;

    console.log("Applying permissions - User:", currentUser, "Role:", currentUserRole);

    // Elements to toggle
    const formSections = document.querySelectorAll('.form-section');
    const previewSection = document.querySelector('.preview-section');
    const settingsBtn = document.getElementById('settings-btn');
    const resetBtn = document.getElementById('reset-btn');
    const usersTabKey = document.getElementById('tab-btn-users');

    // All logged-in users can see manifest UI
    formSections.forEach(el => el.classList.remove('guest-hidden'));
    if (previewSection) previewSection.classList.remove('guest-hidden');
    if (resetBtn) resetBtn.classList.remove('guest-hidden');

    // Settings Button - Admin only
    if (currentUserRole === 'admin') {
        if (settingsBtn) settingsBtn.classList.remove('guest-hidden');
        if (usersTabKey) usersTabKey.style.display = 'block';
    } else {
        if (settingsBtn) settingsBtn.classList.add('guest-hidden');
        if (usersTabKey) usersTabKey.style.display = 'none';
    }
}

// User Management Settings UI
async function renderUsersList() {
    const list = document.getElementById('users-list');
    list.innerHTML = '<div style="text-align:center; padding:1rem;">Loading users...</div>';

    try {
        const response = await fetch(`${API_URL}/users`);
        const data = await response.json();
        const apiUsers = data.users || [];

        list.innerHTML = '';

        apiUsers.forEach((user) => {
            const div = document.createElement('div');
            div.className = 'settings-item';

            const isSelf = currentUser && currentUser.username === user.username;
            const deleteBtn = (user.username !== 'admin' && !isSelf)
                ? `<button class="btn-icon btn-delete" onclick="deleteUser('${user.username}')"><i data-lucide="trash-2"></i></button>`
                : '';

            const roleBadge = user.is_admin ? '<span class="badge" style="background:#fef3c7; color:#d97706;">Admin</span>'
                : (user.can_manifest ? '<span class="badge" style="background:#dcfce7; color:#166534;">Manifest</span>' : '<span class="badge">View Only</span>');

            div.innerHTML = `
                <div class="settings-item-info">
                    <span class="settings-item-name">${user.username}</span>
                    <span class="settings-item-details">${roleBadge}</span>
                </div>
                <div class="settings-item-actions">
                    ${deleteBtn}
                </div>
            `;
            list.appendChild(div);
        });
        lucide.createIcons();
    } catch (error) {
        console.error('Failed to load users:', error);
        list.innerHTML = '<div style="text-align:center; color:red; padding:1rem;">Failed to load users</div>';
    }
}

async function addUser() {
    const nameInput = document.getElementById('new-user-name');
    const pwdInput = document.getElementById('new-user-password');
    const manifestCheck = document.getElementById('new-user-manifest');
    const addBtn = document.getElementById('add-user-btn');

    const username = nameInput.value.trim();
    const password = pwdInput.value.trim();

    if (!username || !password) {
        alert('Username and Password are required');
        return;
    }

    addBtn.disabled = true;
    addBtn.textContent = 'Adding...';

    try {
        const response = await fetch(`${API_URL}/users`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username: username,
                password: password,
                is_admin: false,
                can_manifest: manifestCheck.checked
            })
        });

        if (!response.ok) {
            const error = await response.json();
            alert(error.detail || 'Failed to create user');
            return;
        }

        await renderUsersList();

        nameInput.value = '';
        pwdInput.value = '';
        manifestCheck.checked = false;
    } catch (error) {
        console.error('Failed to add user:', error);
        alert('Failed to add user. Please check the server.');
    } finally {
        addBtn.disabled = false;
        addBtn.textContent = 'Add User';
    }
}

async function deleteUser(username) {
    if (confirm(`Are you sure you want to delete user "${username}"?`)) {
        try {
            const response = await fetch(`${API_URL}/users/${username}`, {
                method: 'DELETE'
            });

            if (!response.ok) {
                alert('Failed to delete user');
                return;
            }

            await renderUsersList();
        } catch (error) {
            console.error('Failed to delete user:', error);
            alert('Failed to delete user. Please check the server.');
        }
    }
}

// Expose deleteUser globally
window.deleteUser = deleteUser;

// Logout Function
function handleLogout() {
    currentUser = null;
    loginIntent = null;

    // Hide any open modals
    document.querySelectorAll('.modal').forEach(m => {
        m.classList.add('hidden');
        m.classList.remove('visible');
    });

    // Show landing page
    showLandingPage();
}

/* Export Options & Print Preview Logic */

function showExportOptions() {
    const modal = document.getElementById('export-options-modal');
    if (modal) {
        modal.classList.remove('hidden');
        modal.classList.add('visible');
    }
}

function hideExportOptions() {
    const modal = document.getElementById('export-options-modal');
    if (modal) {
        modal.classList.add('hidden');
        modal.classList.remove('visible');
    }
}

function showReportPrintPreview() {
    hideExportOptions();
    renderReportPrintPreview();
    const modal = document.getElementById('report-preview-modal');
    if (modal) {
        modal.classList.remove('hidden');
        modal.classList.add('visible');
    }
}

function hideReportPreview() {
    const modal = document.getElementById('report-preview-modal');
    if (modal) {
        modal.classList.add('hidden');
        modal.classList.remove('visible');
    }
}

function renderReportPrintPreview() {
    const container = document.getElementById('printable-report-content');
    if (!container) return;

    // Get filter info for header
    const dateFromInput = document.getElementById('report-date-from');
    const dateToInput = document.getElementById('report-date-to');
    const dateFrom = dateFromInput && dateFromInput.value ? formatDate(dateFromInput.value) : 'Any';
    const dateTo = dateToInput && dateToInput.value ? formatDate(dateToInput.value) : 'Any';
    const routeFilter = document.getElementById('report-route-filter').value || 'All Routes';
    const reportTitle = currentReportView === 'outstanding' ? 'Outstanding Orders Report' : 'Dispatch Report';
    const showManifestCol = currentReportView !== 'outstanding';

    const today = new Date().toLocaleString();

    let html = `
        <div class="print-header">
            <div class="print-logo">
                 <img src="logo.png" alt="BRD" style="display:none;" onerror="this.nextElementSibling.style.display='block'">
                 <span style="font-size:24px; font-weight:bold; color:#1e293b;">BRD Distribution</span>
            </div>
            <div class="print-info">
                <h1 style="margin: 0; font-size: 24px; color: #1e293b;">${reportTitle}</h1>
                <p style="margin: 5px 0 0; color: #64748b;">Generated: ${today}</p>
                <p style="margin: 5px 0 0; color: #64748b; font-size: 13px;">
                    Date Range: ${dateFrom} - ${dateTo} | Route: ${routeFilter}
                </p>
            </div>
        </div>
        
        <table class="print-table">
            <thead>
                <tr>
                    <th>Invoice #</th>
                    <th>Order #</th>
                    ${showManifestCol ? '<th>Manifest #</th>' : ''}
                    <th>Truck Reg</th>
                    <th>Customer Name</th>
                    <th>Invoice Date</th>
                    <th>${currentReportView === 'outstanding' ? 'Status' : 'Date Dispatched'}</th>
                    <th>Driver</th>
                    <th>Assistant</th>
                    <th>Checker</th>
                </tr>
            </thead>
            <tbody>
    `;

    if (currentPrintableData && currentPrintableData.length > 0) {
        currentPrintableData.forEach(row => {
            let dispatchedVal = '-';
            if (currentReportView === 'outstanding') {
                dispatchedVal = 'Pending';
            } else if (row.dateDispatched) {
                // Check if it's already formatted or raw date string
                dispatchedVal = row.dateDispatched.includes('/') ? row.dateDispatched : formatDate(row.dateDispatched);
            }

            html += `
                <tr>
                    <td>${row.invoice}</td>
                    <td>${row.orderNumber}</td>
                    ${showManifestCol ? `<td>${row.manifest || '-'}</td>` : ''}
                    <td>${row.truckReg || '-'}</td>
                    <td>${row.customer}</td>
                    <td>${row.invoiceDate}</td>
                    <td>${dispatchedVal}</td>
                    <td>${row.driver}</td>
                    <td>${row.assistant}</td>
                    <td>${row.checker}</td>
                </tr>
            `;
        });
    } else {
        html += `<tr><td colspan="8" style="text-align:center; padding: 20px;">No data available</td></tr>`;
    }

    html += `
            </tbody>
        </table>
        
        <div style="margin-top: 30px; border-top: 1px solid #cbd5e1; padding-top: 10px; display: flex; justify-content: space-between; font-size: 12px; color: #94a3b8;">
            <span>Delivery Manifest System</span>
            <span>Page 1 of 1</span>
        </div>
    `;

    container.innerHTML = html;
}

// =============================================
// DATE INPUT ENHANCEMENTS
// =============================================
function setDateToToday() {
    const dateInput = document.getElementById('date');
    if (dateInput) {
        // Only set if empty or forcing reset logic
        // But user wants it to specificially default to current date.
        // So checking empty is good for load, but for reset we might want to force it.
        // The reset listener below handles the force reset.
        if (!dateInput.value) {
            const today = new Date().toISOString().split('T')[0];
            dateInput.value = today;
        }
    }
}

function forceDateToToday() {
    const dateInput = document.getElementById('date');
    if (dateInput) {
        const today = new Date().toISOString().split('T')[0];
        dateInput.value = today;
    }
}

// Initialize on load
document.addEventListener('DOMContentLoaded', setDateToToday);
// Just in case DOMContentLoaded already fired or script runs late
setDateToToday();

// Ensure reset button also triggers date reset
const resetBtnRef = document.getElementById('reset-btn');
if (resetBtnRef) {
    resetBtnRef.addEventListener('click', () => {
        // Slight delay to ensure it overrides any clearing done by original reset
        setTimeout(forceDateToToday, 100);
    });
}

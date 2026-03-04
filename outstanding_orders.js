/**
 * Outstanding Orders JavaScript
 * Handles API integration and client-side filtering/sorting
 * 
 * IMPORTANT: This page does NOT calculate which invoices are outstanding.
 * The backend determines outstanding status (invoices with no dispatch record).
 * Client-side logic only filters/sorts the data received from the API.
 */

// API Configuration
// Relative path — works on any host/port (local, LAN, or Render production).
const API_BASE_URL = '/api';
const API_ENDPOINT = `${API_BASE_URL}/reports/outstanding`;

// Safety check: Log resolved API URL on startup
console.log(`[Outstanding Orders] Using relative API paths. Page origin: ${window.location.origin}`);

// State Management
let allOrders = []; // All orders from API
let filteredOrders = []; // Filtered orders for display
let currentSort = {
    field: 'invoice_date',
    order: 'desc' // Default: newest first
};

// DOM Elements
const elements = {
    // States
    loadingState: document.getElementById('loading-state'),
    errorState: document.getElementById('error-state'),
    emptyState: document.getElementById('empty-state'),
    tableContainer: document.getElementById('table-container'),

    // Table
    tableBody: document.getElementById('outstanding-table-body'),

    // Search
    searchInput: document.getElementById('search-input'),
    clearSearchBtn: document.getElementById('clear-search-btn'),

    // Results
    resultsCount: document.getElementById('results-count'),
    resultsInfo: document.getElementById('results-info'),
    filteredCount: document.getElementById('filtered-count'),

    // Navigation
    backBtn: document.getElementById('back-btn'),
    dispatchReportBtn: document.getElementById('dispatch-report-btn'),
    refreshBtn: document.getElementById('refresh-btn'),

    // Error
    errorMessage: document.getElementById('error-message'),
    retryBtn: document.getElementById('retry-btn')
};

/**
 * Initialize the application
 */
function init() {
    setupEventListeners();
    loadData();
}

/**
 * Setup all event listeners
 */
function setupEventListeners() {
    // Search - client-side filtering only
    elements.searchInput.addEventListener('input', handleSearch);
    elements.clearSearchBtn.addEventListener('click', clearSearch);

    // Table sorting - client-side only
    document.querySelectorAll('th[data-sort]').forEach(th => {
        th.addEventListener('click', () => handleSort(th.dataset.sort));
    });

    // Navigation
    elements.backBtn.addEventListener('click', () => window.history.back());
    elements.dispatchReportBtn.addEventListener('click', () => window.location.href = 'dispatch_report.html');
    elements.refreshBtn.addEventListener('click', loadData);

    // Retry button
    elements.retryBtn.addEventListener('click', loadData);
}

/**
 * Handle search input - CLIENT-SIDE FILTERING ONLY
 */
function handleSearch(e) {
    const query = e.target.value.trim().toLowerCase();

    // Show/hide clear button
    if (query) {
        elements.clearSearchBtn.classList.remove('hidden');
    } else {
        elements.clearSearchBtn.classList.add('hidden');
    }

    // Filter orders client-side
    if (query) {
        filteredOrders = allOrders.filter(order => {
            return (
                (order.invoice_number || '').toLowerCase().includes(query) ||
                (order.order_number || '').toLowerCase().includes(query) ||
                (order.customer_name || '').toLowerCase().includes(query)
            );
        });
    } else {
        filteredOrders = [...allOrders];
    }

    // Re-apply current sort
    applySorting();

    // Render filtered results
    renderTable(filteredOrders);
    updateResultsInfo();
}

/**
 * Clear search input
 */
function clearSearch() {
    elements.searchInput.value = '';
    elements.clearSearchBtn.classList.add('hidden');
    filteredOrders = [...allOrders];
    applySorting();
    renderTable(filteredOrders);
    updateResultsInfo();
}

/**
 * Handle table column sorting - CLIENT-SIDE ONLY
 */
function handleSort(field) {
    // Toggle sort order if clicking same column
    if (currentSort.field === field) {
        currentSort.order = currentSort.order === 'asc' ? 'desc' : 'asc';
    } else {
        currentSort.field = field;
        currentSort.order = 'desc';
    }

    updateSortIndicators();
    applySorting();
    renderTable(filteredOrders);
}

/**
 * Apply sorting to filtered orders
 */
function applySorting() {
    filteredOrders.sort((a, b) => {
        let aVal = a[currentSort.field] || '';
        let bVal = b[currentSort.field] || '';

        // Handle date sorting
        if (currentSort.field === 'invoice_date') {
            aVal = new Date(aVal || '1970-01-01');
            bVal = new Date(bVal || '1970-01-01');
        } else {
            // String comparison
            aVal = String(aVal).toLowerCase();
            bVal = String(bVal).toLowerCase();
        }

        if (aVal < bVal) return currentSort.order === 'asc' ? -1 : 1;
        if (aVal > bVal) return currentSort.order === 'asc' ? 1 : -1;
        return 0;
    });
}

/**
 * Update sort indicators in table headers
 */
function updateSortIndicators() {
    document.querySelectorAll('th[data-sort]').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
        if (th.dataset.sort === currentSort.field) {
            th.classList.add(currentSort.order === 'asc' ? 'sort-asc' : 'sort-desc');
        }
    });
}

/**
 * Load data from API
 * IMPORTANT: Backend determines which invoices are outstanding
 */
async function loadData() {
    showLoadingState();

    try {
        const response = await apiFetch(API_ENDPOINT);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();

        // Store all orders from API
        // Backend has already determined these are outstanding
        allOrders = data.orders || [];
        filteredOrders = [...allOrders];

        // Apply default sorting
        applySorting();
        updateSortIndicators();

        // Render data
        renderTable(filteredOrders);
        updateResultsCount();
        updateResultsInfo();

        // Show appropriate state
        if (allOrders.length === 0) {
            showEmptyState();
        } else {
            showTableState();
        }

    } catch (error) {
        console.error('Error loading data:', error);
        showErrorState(error.message);
    }
}

/**
 * Render table rows
 */
function renderTable(orders) {
    elements.tableBody.innerHTML = '';

    if (orders.length === 0 && allOrders.length > 0) {
        // Filtered results are empty but we have data
        const row = document.createElement('tr');
        row.innerHTML = `
            <td colspan="4" style="text-align: center; padding: 2rem; color: var(--text-secondary);">
                No orders match your search criteria
            </td>
        `;
        elements.tableBody.appendChild(row);
        return;
    }

    orders.forEach(order => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${escapeHtml(order.invoice_number || 'N/A')}</td>
            <td>${escapeHtml(order.order_number || 'N/A')}</td>
            <td>${escapeHtml(order.customer_name || 'N/A')}</td>
            <td>${formatDate(order.invoice_date)}</td>
        `;
        elements.tableBody.appendChild(row);
    });
}

/**
 * Update results count
 */
function updateResultsCount() {
    const count = allOrders.length;
    elements.resultsCount.textContent = `${count} outstanding invoice${count !== 1 ? 's' : ''}`;
}

/**
 * Update results info (filtered count)
 */
function updateResultsInfo() {
    if (filteredOrders.length < allOrders.length) {
        elements.resultsInfo.classList.remove('hidden');
        elements.filteredCount.textContent = `Showing ${filteredOrders.length} of ${allOrders.length} invoices`;
    } else {
        elements.resultsInfo.classList.add('hidden');
    }
}

/**
 * Show loading state
 */
function showLoadingState() {
    elements.loadingState.classList.remove('hidden');
    elements.errorState.classList.add('hidden');
    elements.emptyState.classList.add('hidden');
    elements.tableContainer.classList.add('hidden');
    elements.resultsInfo.classList.add('hidden');

    // Disable interactive elements during loading
    elements.refreshBtn.disabled = true;
}

/**
 * Show error state
 */
function showErrorState(message) {
    elements.loadingState.classList.add('hidden');
    elements.errorState.classList.remove('hidden');
    elements.emptyState.classList.add('hidden');
    elements.tableContainer.classList.add('hidden');
    elements.resultsInfo.classList.add('hidden');

    elements.errorMessage.textContent = message || 'Failed to load outstanding orders. Please check your connection and try again.';

    // Re-enable interactive elements
    elements.refreshBtn.disabled = false;
}

/**
 * Show empty state
 */
function showEmptyState() {
    elements.loadingState.classList.add('hidden');
    elements.errorState.classList.add('hidden');
    elements.emptyState.classList.remove('hidden');
    elements.tableContainer.classList.add('hidden');
    elements.resultsInfo.classList.add('hidden');

    // Re-enable interactive elements
    elements.refreshBtn.disabled = false;
}

/**
 * Show table state
 */
function showTableState() {
    elements.loadingState.classList.add('hidden');
    elements.errorState.classList.add('hidden');
    elements.emptyState.classList.add('hidden');
    elements.tableContainer.classList.remove('hidden');

    // Re-enable interactive elements
    elements.refreshBtn.disabled = false;
}

/**
 * Format date for display
 */
function formatDate(dateString) {
    if (!dateString || dateString === 'N/A') return 'N/A';
    try {
        const date = new Date(dateString);
        return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    } catch {
        return dateString;
    }
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => { requireAuth(); init(); });
} else {
    requireAuth();
    init();
}

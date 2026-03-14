/**
 * delivery_status.js
 *
 * Office/admin visibility into driver delivery progress.
 * Requires: auth.js (loaded before this script via <script src="auth.js">)
 *
 * API used:
 *   GET /api/delivery/manifests               — manifest list with summary
 *   GET /api/delivery/manifests/{number}       — invoice-level detail
 */

const DS_API = '/api/delivery';

let dsState = {
    manifests: [],   // all manifests from last API load
    search:       '',
    dateFrom:     '',
    dateTo:       '',
    statusFilter: '',
    loading:      false,
};

// ── Status badge helpers ───────────────────────────────────────────────────

function manifestStatusBadge(status) {
    const cfg = {
        'PENDING':               { bg: '#94a3b8', label: 'Pending' },
        'IN_PROGRESS':           { bg: '#f59e0b', label: 'In Progress' },
        'COMPLETED':             { bg: '#10b981', label: 'Completed' },
        'COMPLETED_WITH_ISSUES': { bg: '#ef4444', label: 'Issues' },
    };
    const s = cfg[status] || { bg: '#94a3b8', label: status || '—' };
    return `<span class="ds-badge" style="background:${s.bg}">${s.label}</span>`;
}

function invoiceStatusBadge(status) {
    const cfg = {
        'PENDING':    { bg: '#94a3b8', label: 'Pending' },
        'IN_TRANSIT': { bg: '#3b82f6', label: 'In Transit' },
        'DELIVERED':  { bg: '#10b981', label: 'Delivered' },
        'FAILED':     { bg: '#ef4444', label: 'Failed' },
        'PARTIAL':    { bg: '#f59e0b', label: 'Partial' },
        'RETURNED':   { bg: '#8b5cf6', label: 'Returned' },
    };
    const s = cfg[status] || { bg: '#94a3b8', label: status || 'Pending' };
    return `<span class="ds-badge" style="background:${s.bg}">${s.label}</span>`;
}

// ── Utility ────────────────────────────────────────────────────────────────

function dsFormatDate(val) {
    if (!val) return '—';
    try {
        const d = new Date(val);
        if (isNaN(d)) return String(val);
        return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
    } catch { return String(val); }
}

function dsEsc(str) {
    const d = document.createElement('div');
    d.textContent = (str != null) ? String(str) : '';
    return d.innerHTML;
}

// ── Load manifests from API ────────────────────────────────────────────────

async function loadManifests() {
    if (dsState.loading) return;
    dsState.loading = true;
    showDsState('loading');

    try {
        const params = new URLSearchParams();
        if (dsState.dateFrom)     params.append('date_from', dsState.dateFrom);
        if (dsState.dateTo)       params.append('date_to',   dsState.dateTo);
        if (dsState.statusFilter) params.append('status',    dsState.statusFilter);

        const res = await apiFetch(`${DS_API}/manifests?${params}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);

        const data = await res.json();
        dsState.manifests = data.manifests || [];
        renderManifests(filterBySearch(dsState.manifests));

    } catch (e) {
        console.error('[DeliveryStatus] Error loading manifests:', e);
        const msgEl = document.getElementById('ds-error-msg');
        if (msgEl) msgEl.textContent = e.message;
        showDsState('error');
    } finally {
        dsState.loading = false;
    }
}

// ── Client-side search filter ──────────────────────────────────────────────

function filterBySearch(manifests) {
    const q = dsState.search.toLowerCase().trim();
    if (!q) return manifests;
    return manifests.filter(m =>
        (m.manifest_number || '').toLowerCase().includes(q) ||
        (m.driver          || '').toLowerCase().includes(q) ||
        (m.reg_number      || '').toLowerCase().includes(q)
    );
}

// ── Render manifest table ──────────────────────────────────────────────────

function renderManifests(manifests) {
    const tbody = document.getElementById('ds-table-body');
    tbody.innerHTML = '';

    if (!manifests.length) {
        showDsState('empty');
        document.getElementById('ds-results-count').textContent = '';
        return;
    }

    manifests.forEach(m => {
        const s   = m.delivery_summary || {};
        const row = document.createElement('tr');
        row.style.cursor = 'pointer';
        row.title = 'Click to view invoice details';

        row.innerHTML = `
            <td><strong>${dsEsc(m.manifest_number)}</strong></td>
            <td>${dsFormatDate(m.date_dispatched)}</td>
            <td>${dsEsc(m.driver || '—')}</td>
            <td>${dsEsc(m.reg_number || '—')}</td>
            <td class="ds-num">${m.total_items ?? '—'}</td>
            <td class="ds-num ds-delivered">${s.delivered ?? '—'}</td>
            <td class="ds-num ds-transit">${s.in_transit ?? '—'}</td>
            <td class="ds-num ds-pending">${s.pending ?? '—'}</td>
            <td class="ds-num ds-failed">${s.failed ?? '—'}</td>
            <td class="ds-num ds-partial">${s.partial ?? '—'}</td>
            <td>${manifestStatusBadge(s.status || 'PENDING')}</td>
            <td>
                <button class="report-btn report-btn-secondary ds-detail-btn"
                        data-manifest="${dsEsc(m.manifest_number)}">
                    <i data-lucide="eye"></i> Details
                </button>
            </td>
        `;

        // Row click (not on button — button has its own listener via event delegation)
        row.addEventListener('click', (e) => {
            if (e.target.closest('.ds-detail-btn')) return;
            openDetail(m.manifest_number);
        });

        tbody.appendChild(row);
    });

    showDsState('table');
    lucide.createIcons();

    document.getElementById('ds-results-count').textContent =
        `${manifests.length} manifest${manifests.length !== 1 ? 's' : ''} found`;
}

// ── Detail modal ───────────────────────────────────────────────────────────

async function openDetail(manifestNumber) {
    const modal = document.getElementById('ds-detail-modal');
    const body  = document.getElementById('ds-modal-body');

    document.getElementById('ds-modal-title').textContent = `Manifest ${manifestNumber}`;
    body.innerHTML = `
        <div class="ds-modal-loading">
            <div class="spinner"></div>
            <p>Loading invoice details&hellip;</p>
        </div>`;
    modal.classList.remove('hidden');

    try {
        const res = await apiFetch(`${DS_API}/manifests/${encodeURIComponent(manifestNumber)}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        renderDetailModal(data);
    } catch (e) {
        body.innerHTML = `
            <div class="error-state">
                <i data-lucide="alert-triangle"></i>
                <h3>Error Loading Details</h3>
                <p>${dsEsc(e.message)}</p>
            </div>`;
        lucide.createIcons();
    }
}

function renderDetailModal(manifest) {
    const body  = document.getElementById('ds-modal-body');
    const items = manifest.items || [];

    const rowsHtml = items.length
        ? items.map(item => `
            <tr>
                <td>${dsEsc(item.invoice_number || '—')}</td>
                <td>${dsEsc(item.customer_name  || '—')}</td>
                <td>${dsEsc(item.area           || '—')}</td>
                <td>${invoiceStatusBadge(item.delivery_status || 'PENDING')}</td>
                <td class="ds-notes">${dsEsc(item.notes || '—')}</td>
                <td>${item.updated_at ? dsFormatDate(item.updated_at) : '—'}</td>
            </tr>`).join('')
        : `<tr><td colspan="6" style="text-align:center;color:#94a3b8">No invoices found</td></tr>`;

    body.innerHTML = `
        <div class="ds-modal-info">
            <span><strong>Driver:</strong> ${dsEsc(manifest.driver || '—')}</span>
            <span><strong>Date:</strong> ${dsFormatDate(manifest.date_dispatched)}</span>
            <span><strong>Overall Status:</strong> ${manifestStatusBadge(manifest.manifest_status || 'PENDING')}</span>
            <span><strong>Invoices:</strong> ${items.length}</span>
        </div>
        <div class="table-container ds-detail-table">
            <table>
                <thead>
                    <tr>
                        <th>Invoice #</th>
                        <th>Customer</th>
                        <th>Area</th>
                        <th>Delivery Status</th>
                        <th>Notes</th>
                        <th>Last Updated</th>
                    </tr>
                </thead>
                <tbody>${rowsHtml}</tbody>
            </table>
        </div>`;
    lucide.createIcons();
}

function closeDetail() {
    document.getElementById('ds-detail-modal').classList.add('hidden');
}

// ── UI state management ────────────────────────────────────────────────────

function showDsState(which) {
    ['loading', 'error', 'empty', 'table'].forEach(s => {
        const el = document.getElementById(`ds-${s}-state`);
        if (el) el.classList.toggle('hidden', s !== which);
    });
}

// ── Filter handlers ────────────────────────────────────────────────────────

function applyFilters() {
    dsState.dateFrom     = document.getElementById('ds-date-from').value;
    dsState.dateTo       = document.getElementById('ds-date-to').value;
    dsState.statusFilter = document.getElementById('ds-status-filter').value;
    loadManifests();
}

function resetFilters() {
    ['ds-date-from', 'ds-date-to', 'ds-search'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    document.getElementById('ds-status-filter').value = '';
    dsState.search       = '';
    dsState.dateFrom     = '';
    dsState.dateTo       = '';
    dsState.statusFilter = '';
    loadManifests();
}

function handleSearch() {
    dsState.search = document.getElementById('ds-search').value;
    renderManifests(filterBySearch(dsState.manifests));
}

// ── Init ───────────────────────────────────────────────────────────────────

function dsInit() {
    requireAuth();

    document.getElementById('ds-apply-btn').addEventListener('click', applyFilters);
    document.getElementById('ds-reset-btn').addEventListener('click', resetFilters);
    document.getElementById('ds-search').addEventListener('input', handleSearch);
    document.getElementById('ds-close-modal-btn').addEventListener('click', closeDetail);
    document.getElementById('ds-back-btn').addEventListener('click',
        () => window.location.href = 'index.html');

    // Event delegation for Detail buttons inside the table
    document.getElementById('ds-table-body').addEventListener('click', (e) => {
        const btn = e.target.closest('.ds-detail-btn');
        if (btn) openDetail(btn.dataset.manifest);
    });

    // Close modal on backdrop click
    document.getElementById('ds-detail-modal').addEventListener('click', (e) => {
        if (e.target === document.getElementById('ds-detail-modal')) closeDetail();
    });

    loadManifests();
}

document.addEventListener('DOMContentLoaded', dsInit);

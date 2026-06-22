/**
 * analytics.js
 *
 * Phase 1 Analytics page — office-only (ADMIN, DISPATCH, REPORTS_ONLY).
 * Depends on auth.js being loaded first (provides apiFetch, getUserRole, requireAuth).
 */

'use strict';

// ── Constants ─────────────────────────────────────────────────────────────

const API_BASE   = '/api/analytics';
const ROUTE_API  = '/api/customer-routes';

// ── Auth / Access ─────────────────────────────────────────────────────────

/**
 * Redirect DRIVER users away from this office-only page.
 * Defined per-page following project convention (not in auth.js).
 */
function requireOfficeRole() {
    if (getUserRole() === 'DRIVER') {
        window.location.href = 'delivery_status.html';
    }
}

// ── Filter State ──────────────────────────────────────────────────────────

let filterState = {
    dateFrom : '',
    dateTo   : '',
    search   : '',
    route    : '',
};

// ── Section Pagination State ──────────────────────────────────────────────

let manifestsState      = { offset: 0, limit: 25, total: 0 };
let driversState        = { offset: 0, limit: 25, total: 0 };
let exceptionsState     = { offset: 0, limit: 25, total: 0, status: '' };
let valueManifestsState = { offset: 0, limit: 25, total: 0 };
let valueTrucksState    = { offset: 0, limit: 25, total: 0 };

// ── Utilities ─────────────────────────────────────────────────────────────

function escapeHtml(str) {
    if (str == null) return '';
    return String(str)
        .replace(/&/g,  '&amp;')
        .replace(/</g,  '&lt;')
        .replace(/>/g,  '&gt;')
        .replace(/"/g,  '&quot;')
        .replace(/'/g,  '&#39;');
}

function fmtNum(v) {
    if (v === null || v === undefined) return '0';
    return String(v);
}

function fmtPct(v) {
    if (v === null || v === undefined) return '—';
    return v + '%';
}

function fmtMoney(v) {
    if (v === null || v === undefined) return '—';
    return Number(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/**
 * Generate and trigger a UTF-8 CSV download.
 * @param {string}   filename  e.g. 'manifests-2025-03-25.csv'
 * @param {string[]} headers   Column header labels
 * @param {Array[]}  rows      Array of value arrays, one per data row
 */
function exportCsv(filename, headers, rows) {
    const esc   = v => '"' + String(v === null || v === undefined ? '' : v).replace(/"/g, '""') + '"';
    const lines = [headers.map(esc).join(',')];
    rows.forEach(r => lines.push(r.map(esc).join(',')));
    const blob = new Blob([lines.join('\r\n')], { type: 'text/csv;charset=utf-8;' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
}

/**
 * Show one of four UI states for a section.
 * @param {string} prefix  e.g. 'manifests'
 * @param {string} which   'loading' | 'error' | 'empty' | 'content'
 */
function showState(prefix, which) {
    ['loading', 'error', 'empty', 'content'].forEach(state => {
        const el = document.getElementById(`${prefix}-${state}`);
        if (!el) return;
        if (state === which) el.classList.remove('hidden');
        else                 el.classList.add('hidden');
    });
}

/**
 * Build a URLSearchParams string from the current filterState plus
 * any extra key/value pairs (e.g. limit, offset, status).
 */
function buildParams(extra) {
    const p = new URLSearchParams();
    if (filterState.dateFrom) p.set('date_from', filterState.dateFrom);
    if (filterState.dateTo)   p.set('date_to',   filterState.dateTo);
    if (filterState.search)   p.set('search',    filterState.search);
    if (filterState.route)    p.set('route',     filterState.route);
    if (extra) {
        for (const [k, v] of Object.entries(extra)) {
            if (v !== null && v !== undefined && v !== '') p.set(k, String(v));
        }
    }
    return p.toString();
}

// ── Status Badge Renderers ────────────────────────────────────────────────

const _MANIFEST_BADGE = {
    'PENDING'               : 'badge-pending',
    'IN_PROGRESS'           : 'badge-in-progress',
    'COMPLETED'             : 'badge-completed',
    'COMPLETED_WITH_ISSUES' : 'badge-completed-issues',
};

const _DELIVERY_BADGE = {
    'PENDING'    : 'badge-pending',
    'IN_TRANSIT' : 'badge-in-transit',
    'DELIVERED'  : 'badge-delivered',
    'FAILED'     : 'badge-failed',
    'PARTIAL'    : 'badge-partial',
    'RETURNED'   : 'badge-returned',
};

function manifestStatusBadge(status) {
    const cls   = _MANIFEST_BADGE[status] || 'badge-pending';
    const label = (status || 'PENDING').replace(/_/g, ' ');
    return `<span class="status-badge ${cls}">${escapeHtml(label)}</span>`;
}

/**
 * Exception status badge.
 * Critical rule: if the backend returns status='DELIVERED' AND pod_present===false,
 * the row is a missing-PoD case — display "MISSING PoD", not "DELIVERED".
 */
function exceptionStatusBadge(status, podPresent) {
    if (status === 'DELIVERED' && podPresent === false) {
        return `<span class="status-badge badge-missing-pod">MISSING PoD</span>`;
    }
    const cls = _DELIVERY_BADGE[status] || 'badge-pending';
    return `<span class="status-badge ${cls}">${escapeHtml(status || '—')}</span>`;
}

// ── Pagination ────────────────────────────────────────────────────────────

/**
 * Render numbered page buttons into a container div.
 * Shows up to (currentPage ± 2) plus always first and last pages, with
 * ellipsis gaps.
 */
function renderPageNumbers(containerId, currentPage, totalPages, onPageClick) {
    const container = document.getElementById(containerId);
    if (!container) return;
    container.innerHTML = '';
    if (totalPages <= 1) return;

    const delta = 2;
    const pages = new Set([1, totalPages]);
    for (let i = currentPage - delta; i <= currentPage + delta; i++) {
        if (i >= 1 && i <= totalPages) pages.add(i);
    }
    const sorted = [...pages].sort((a, b) => a - b);

    let prev = null;
    sorted.forEach(page => {
        if (prev !== null && page - prev > 1) {
            const gap  = document.createElement('span');
            gap.className   = 'page-btn ellipsis';
            gap.textContent = '…';
            container.appendChild(gap);
        }
        const btn = document.createElement('button');
        btn.className   = 'page-btn' + (page === currentPage ? ' active' : '');
        btn.textContent = page;
        btn.addEventListener('click', () => onPageClick(page));
        container.appendChild(btn);
        prev = page;
    });
}

/**
 * Update all pagination controls for a section.
 * @param {string}   prefix       e.g. 'manifests'
 * @param {object}   state        Section state object {offset, limit, total}
 * @param {function} onPageChange Called with new offset when a page is chosen
 */
function updatePagination(prefix, state, onPageChange) {
    const { offset, limit, total } = state;
    const totalPages  = total > 0 ? Math.ceil(total / limit) : 1;
    const currentPage = Math.floor(offset / limit) + 1;

    const container = document.getElementById(`${prefix}-pagination-container`);
    if (container) {
        if (total <= limit) container.classList.add('hidden');
        else                container.classList.remove('hidden');
    }

    const infoEl = document.getElementById(`${prefix}-pagination-info`);
    if (infoEl) {
        const from = total === 0 ? 0 : offset + 1;
        const to   = Math.min(offset + limit, total);
        infoEl.textContent = `Showing ${from}–${to} of ${total}`;
    }

    const firstBtn = document.getElementById(`${prefix}-first-btn`);
    const prevBtn  = document.getElementById(`${prefix}-prev-btn`);
    const nextBtn  = document.getElementById(`${prefix}-next-btn`);
    const lastBtn  = document.getElementById(`${prefix}-last-btn`);

    if (firstBtn) firstBtn.disabled = currentPage === 1;
    if (prevBtn)  prevBtn.disabled  = currentPage === 1;
    if (nextBtn)  nextBtn.disabled  = currentPage === totalPages;
    if (lastBtn)  lastBtn.disabled  = currentPage === totalPages;

    if (firstBtn) firstBtn.onclick = () => onPageChange(0);
    if (prevBtn)  prevBtn.onclick  = () => onPageChange(Math.max(0, offset - limit));
    if (nextBtn)  nextBtn.onclick  = () => onPageChange(Math.min((totalPages - 1) * limit, offset + limit));
    if (lastBtn)  lastBtn.onclick  = () => onPageChange((totalPages - 1) * limit);

    renderPageNumbers(`${prefix}-page-numbers`, currentPage, totalPages, page => {
        onPageChange((page - 1) * limit);
    });
}

// ── Route Filter ──────────────────────────────────────────────────────────

async function loadRouteFilter() {
    try {
        const res = await apiFetch(ROUTE_API);
        if (!res.ok) return;
        const data = await res.json();
        const routes = data.routes || [];
        const select = document.getElementById('filter-route');
        if (!select) return;
        const names = [...new Set(routes.map(r => r.route_name).filter(Boolean))].sort();
        names.forEach(name => {
            const opt = document.createElement('option');
            opt.value       = name;
            opt.textContent = name;
            select.appendChild(opt);
        });
    } catch (e) {
        console.warn('[Analytics] Could not load routes for filter:', e.message);
    }
}

// ── Overview / KPI Cards ──────────────────────────────────────────────────

async function loadOverview() {
    try {
        const res = await apiFetch(`${API_BASE}/overview?${buildParams()}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        renderKpiCards(await res.json());
    } catch (e) {
        console.error('[Analytics] Overview error:', e.message);
        renderKpiCards(null);
    }
}

function renderKpiCards(d) {
    const set = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = val;
    };
    if (!d) {
        ['kpi-manifests', 'kpi-invoices', 'kpi-delivered',
         'kpi-completion', 'kpi-pod', 'kpi-missing-pod'].forEach(id => set(id, '—'));
        return;
    }
    set('kpi-manifests',   fmtNum(d.total_manifests));
    set('kpi-invoices',    fmtNum(d.total_invoices));
    set('kpi-delivered',   fmtNum(d.delivered));
    set('kpi-completion',  fmtPct(d.completion_rate));
    set('kpi-pod',         fmtPct(d.pod_compliance_rate));
    set('kpi-missing-pod', fmtNum(d.missing_pod_count));
}

// ── Manifests Table ───────────────────────────────────────────────────────

async function loadManifests(offset) {
    if (offset !== undefined && offset !== null) manifestsState.offset = offset;
    showState('manifests', 'loading');

    try {
        const page = Math.floor(manifestsState.offset / manifestsState.limit) + 1;
        const qs   = buildParams({ page, page_size: manifestsState.limit });
        const res  = await apiFetch(`${API_BASE}/manifests?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        manifestsState.total = data.total || 0;

        const countEl = document.getElementById('manifests-count');
        if (countEl) countEl.textContent = `${manifestsState.total} manifests`;

        if (!data.items || data.items.length === 0) {
            showState('manifests', 'empty');
            updatePagination('manifests', manifestsState, loadManifests);
            return;
        }

        renderManifestsTable(data.items);
        updatePagination('manifests', manifestsState, loadManifests);
        showState('manifests', 'content');

    } catch (e) {
        console.error('[Analytics] Manifests error:', e.message);
        const errEl = document.getElementById('manifests-error-msg');
        if (errEl) errEl.textContent = e.message;
        showState('manifests', 'error');
    }
}

function renderManifestsTable(rows) {
    const tbody = document.getElementById('manifests-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    rows.forEach(r => {
        const issues = (r.failed || 0) + (r.partial || 0);
        const issuesHtml = issues > 0
            ? `<span style="color:var(--danger-color);font-weight:600">${issues}</span>`
            : '0';
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(r.manifest_number)}</td>
            <td>${escapeHtml(r.dispatch_date || '—')}</td>
            <td>${escapeHtml(r.driver || 'Unassigned')}</td>
            <td>${escapeHtml(r.route || '—')}</td>
            <td class="num-cell">${fmtNum(r.total_invoices)}</td>
            <td class="num-cell">${fmtNum(r.delivered)}</td>
            <td class="num-cell">${fmtNum(r.in_transit)}</td>
            <td class="num-cell">${fmtNum(r.pending)}</td>
            <td class="num-cell">${issuesHtml}</td>
            <td>${manifestStatusBadge(r.manifest_status)}</td>
        `;
        tbody.appendChild(tr);
    });
}

// ── Drivers Table ─────────────────────────────────────────────────────────

async function loadDrivers(offset) {
    if (offset !== undefined && offset !== null) driversState.offset = offset;
    showState('drivers', 'loading');

    try {
        const page = Math.floor(driversState.offset / driversState.limit) + 1;
        const qs   = buildParams({ page, page_size: driversState.limit });
        const res  = await apiFetch(`${API_BASE}/drivers?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        driversState.total = data.total || 0;

        const countEl = document.getElementById('drivers-count');
        if (countEl) countEl.textContent = `${driversState.total} drivers`;

        if (!data.items || data.items.length === 0) {
            showState('drivers', 'empty');
            updatePagination('drivers', driversState, loadDrivers);
            return;
        }

        renderDriversTable(data.items);
        updatePagination('drivers', driversState, loadDrivers);
        showState('drivers', 'content');

    } catch (e) {
        console.error('[Analytics] Drivers error:', e.message);
        const errEl = document.getElementById('drivers-error-msg');
        if (errEl) errEl.textContent = e.message;
        showState('drivers', 'error');
    }
}

function renderDriversTable(rows) {
    const tbody = document.getElementById('drivers-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    rows.forEach(r => {
        const failedHtml   = r.failed  > 0 ? `<span style="color:var(--danger-color)">${r.failed}</span>`  : '0';
        const partialHtml  = r.partial > 0 ? `<span style="color:var(--warning-color)">${r.partial}</span>` : '0';
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(r.driver || 'Unassigned')}</td>
            <td class="num-cell">${fmtNum(r.manifests_assigned)}</td>
            <td class="num-cell">${fmtNum(r.total_invoices)}</td>
            <td class="num-cell">${fmtNum(r.delivered)}</td>
            <td class="num-cell">${failedHtml}</td>
            <td class="num-cell">${partialHtml}</td>
            <td class="num-cell">${fmtPct(r.success_rate)}</td>
            <td class="num-cell">${fmtPct(r.pod_compliance_rate)}</td>
        `;
        tbody.appendChild(tr);
    });
}

// ── Routes / Areas ────────────────────────────────────────────────────────

async function loadRoutes() {
    showState('routes', 'loading');

    try {
        const qs  = buildParams();
        const res = await apiFetch(`${API_BASE}/routes?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        const routeRows = data.route_results || [];
        const areaRows  = data.area_results  || [];

        const countEl = document.getElementById('routes-count');
        if (countEl) countEl.textContent = `${routeRows.length} routes · ${areaRows.length} areas`;

        if (routeRows.length === 0 && areaRows.length === 0) {
            showState('routes', 'empty');
            return;
        }

        renderRoutesTable(routeRows);
        renderAreasTable(areaRows);
        showState('routes', 'content');

    } catch (e) {
        console.error('[Analytics] Routes error:', e.message);
        const errEl = document.getElementById('routes-error-msg');
        if (errEl) errEl.textContent = e.message;
        showState('routes', 'error');
    }
}

function renderRoutesTable(rows) {
    const tbody = document.getElementById('routes-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    if (!rows.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td colspan="9" style="text-align:center;color:var(--text-secondary);padding:1rem">No route data</td>`;
        tbody.appendChild(tr);
        return;
    }
    rows.forEach(r => {
        const issues     = (r.failed || 0) + (r.partial || 0) + (r.returned || 0);
        const issuesHtml = issues > 0
            ? `<span style="color:var(--danger-color);font-weight:600">${issues}</span>`
            : '0';
        const modeHtml = r.delivery_mode
            ? `<span style="font-size:0.75rem;color:var(--text-secondary)">${escapeHtml(r.delivery_mode)}</span>`
            : '<span style="color:var(--text-secondary)">—</span>';
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(r.route_name)}</td>
            <td>${modeHtml}</td>
            <td class="num-cell">${fmtNum(r.total_invoices)}</td>
            <td class="num-cell">${fmtNum(r.delivered)}</td>
            <td class="num-cell">${fmtNum(r.in_transit)}</td>
            <td class="num-cell">${fmtNum(r.pending)}</td>
            <td class="num-cell">${issuesHtml}</td>
            <td class="num-cell">${fmtPct(r.completion_rate)}</td>
            <td class="num-cell">${fmtPct(r.pod_compliance_rate)}</td>
        `;
        tbody.appendChild(tr);
    });
}

function renderAreasTable(rows) {
    const tbody = document.getElementById('areas-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    if (!rows.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td colspan="7" style="text-align:center;color:var(--text-secondary);padding:1rem">No area data</td>`;
        tbody.appendChild(tr);
        return;
    }
    rows.forEach(r => {
        const issues     = (r.failed || 0) + (r.partial || 0) + (r.returned || 0);
        const issuesHtml = issues > 0
            ? `<span style="color:var(--danger-color);font-weight:600">${issues}</span>`
            : '0';
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(r.area)}</td>
            <td class="num-cell">${fmtNum(r.total_invoices)}</td>
            <td class="num-cell">${fmtNum(r.delivered)}</td>
            <td class="num-cell">${fmtNum(r.in_transit)}</td>
            <td class="num-cell">${fmtNum(r.pending)}</td>
            <td class="num-cell">${issuesHtml}</td>
            <td class="num-cell">${fmtPct(r.completion_rate)}</td>
        `;
        tbody.appendChild(tr);
    });
}

// ── Trends ────────────────────────────────────────────────────────────────

async function loadTrends() {
    showState('trends', 'loading');

    try {
        const granEl = document.getElementById('trends-granularity');
        const gran   = granEl ? granEl.value : 'day';
        const qs     = buildParams({ granularity: gran });
        const res    = await apiFetch(`${API_BASE}/trends?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        const rows = data.series || [];

        const countEl = document.getElementById('trends-count');
        if (countEl) countEl.textContent = `${rows.length} period${rows.length !== 1 ? 's' : ''}`;

        if (!rows.length) {
            showState('trends', 'empty');
            return;
        }

        renderTrendsTable(rows);
        showState('trends', 'content');

    } catch (e) {
        console.error('[Analytics] Trends error:', e.message);
        const errEl = document.getElementById('trends-error-msg');
        if (errEl) errEl.textContent = e.message;
        showState('trends', 'error');
    }
}

function renderTrendsTable(rows) {
    const tbody = document.getElementById('trends-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    rows.forEach(r => {
        const unresolvedHtml = (r.unresolved || 0) > 0
            ? `<span style="color:var(--warning-color,#f59e0b);font-weight:600">${fmtNum(r.unresolved)}</span>`
            : fmtNum(r.unresolved);
        const exceptionsHtml = (r.exceptions || 0) > 0
            ? `<span style="color:var(--danger-color);font-weight:600">${fmtNum(r.exceptions)}</span>`
            : fmtNum(r.exceptions);
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(r.period || '—')}</td>
            <td class="num-cell">${fmtNum(r.manifests_dispatched)}</td>
            <td class="num-cell">${fmtNum(r.invoices_dispatched)}</td>
            <td class="num-cell">${fmtNum(r.delivered)}</td>
            <td class="num-cell">${unresolvedHtml}</td>
            <td class="num-cell">${exceptionsHtml}</td>
            <td class="num-cell">${fmtNum(r.pod_uploads)}</td>
        `;
        tbody.appendChild(tr);
    });
}

// ── Exceptions Table ──────────────────────────────────────────────────────

async function loadExceptions(offset) {
    if (offset !== undefined && offset !== null) exceptionsState.offset = offset;
    showState('exceptions', 'loading');

    try {
        const page = Math.floor(exceptionsState.offset / exceptionsState.limit) + 1;
        const qs   = buildParams({ page, page_size: exceptionsState.limit, status: exceptionsState.status });
        const res  = await apiFetch(`${API_BASE}/exceptions?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        exceptionsState.total = data.total || 0;

        const countEl = document.getElementById('exceptions-count');
        if (countEl) countEl.textContent = `${exceptionsState.total} exceptions`;

        if (!data.items || data.items.length === 0) {
            showState('exceptions', 'empty');
            updatePagination('exceptions', exceptionsState, loadExceptions);
            return;
        }

        renderExceptionsTable(data.items);
        updatePagination('exceptions', exceptionsState, loadExceptions);
        showState('exceptions', 'content');

    } catch (e) {
        console.error('[Analytics] Exceptions error:', e.message);
        const errEl = document.getElementById('exceptions-error-msg');
        if (errEl) errEl.textContent = e.message;
        showState('exceptions', 'error');
    }
}

function renderExceptionsTable(rows) {
    const tbody = document.getElementById('exceptions-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    rows.forEach(r => {
        const ageDays = (r.age_days !== null && r.age_days !== undefined) ? r.age_days : '—';
        const podIcon = r.pod_present
            ? `<span style="color:var(--success-color);font-size:1rem">✓</span>`
            : `<span style="color:var(--danger-color);font-size:1rem">✗</span>`;
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(r.invoice_number)}</td>
            <td>${escapeHtml(r.manifest_number)}</td>
            <td>${escapeHtml(r.customer_name || '—')}</td>
            <td>${escapeHtml(r.area || '—')}</td>
            <td>${escapeHtml(r.driver || 'Unassigned')}</td>
            <td>${exceptionStatusBadge(r.status, r.pod_present)}</td>
            <td class="num-cell">${escapeHtml(String(ageDays))}</td>
            <td style="text-align:center">${podIcon}</td>
        `;
        tbody.appendChild(tr);
    });
}

// ── Aging Summary ─────────────────────────────────────────────────────────

async function loadAging() {
    showState('aging', 'loading');

    try {
        const res = await apiFetch(`${API_BASE}/aging?${buildParams()}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        const invoiceRows  = data.invoice_aging  || [];
        const manifestRows = data.manifest_aging || [];

        // Show empty state only if both sides are entirely zero
        const hasData = invoiceRows.some(r => r.count > 0) || manifestRows.some(r => r.count > 0);
        if (!hasData) {
            showState('aging', 'empty');
            return;
        }

        renderAgingTable('aging-invoice-tbody',  invoiceRows);
        renderAgingTable('aging-manifest-tbody', manifestRows);
        showState('aging', 'content');

    } catch (e) {
        console.error('[Analytics] Aging error:', e.message);
        const errEl = document.getElementById('aging-error-msg');
        if (errEl) errEl.textContent = e.message;
        showState('aging', 'error');
    }
}

function renderAgingTable(tbodyId, rows) {
    const tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    tbody.innerHTML = '';
    if (!rows.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td colspan="3" style="text-align:center;color:var(--text-secondary);padding:1rem">No data</td>`;
        tbody.appendChild(tr);
        return;
    }
    rows.forEach(r => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(r.bucket)} days</td>
            <td class="num-cell">${fmtNum(r.count)}</td>
            <td class="num-cell">${fmtPct(r.pct)}</td>
        `;
        tbody.appendChild(tr);
    });
}

// ── Export Functions ──────────────────────────────────────────────────────

async function exportTrends() {
    const btn = document.getElementById('trends-export-btn');
    if (btn) btn.disabled = true;
    try {
        const granEl = document.getElementById('trends-granularity');
        const gran   = granEl ? granEl.value : 'day';
        const qs     = buildParams({ granularity: gran });
        const res    = await apiFetch(`${API_BASE}/trends?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data   = await res.json();
        const rows   = data.series || [];
        const date   = new Date().toISOString().slice(0, 10);
        exportCsv(`trends-${date}.csv`,
            ['Period', 'Manifests Dispatched', 'Invoices Dispatched', 'Delivered', 'Unresolved', 'Exceptions', 'PoD Uploads'],
            rows.map(r => [r.period, r.manifests_dispatched, r.invoices_dispatched, r.delivered, r.unresolved, r.exceptions, r.pod_uploads])
        );
    } catch (e) {
        console.error('[Analytics] Export trends failed:', e.message);
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function exportRoutes() {
    const btn = document.getElementById('routes-export-btn');
    if (btn) btn.disabled = true;
    try {
        const qs   = buildParams();
        const res  = await apiFetch(`${API_BASE}/routes?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const rows = data.route_results || [];
        const date = new Date().toISOString().slice(0, 10);
        exportCsv(`routes-${date}.csv`,
            ['Route', 'Delivery Mode', 'Total Invoices', 'Delivered', 'In Transit', 'Pending', 'Failed', 'Partial', 'Returned', 'Completion Rate', 'PoD Rate'],
            rows.map(r => [r.route_name, r.delivery_mode || '—', r.total_invoices, r.delivered, r.in_transit, r.pending, r.failed, r.partial, r.returned, r.completion_rate, r.pod_compliance_rate])
        );
    } catch (e) {
        console.error('[Analytics] Export routes failed:', e.message);
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function exportAreas() {
    const btn = document.getElementById('areas-export-btn');
    if (btn) btn.disabled = true;
    try {
        const qs   = buildParams();
        const res  = await apiFetch(`${API_BASE}/routes?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const rows = data.area_results || [];
        const date = new Date().toISOString().slice(0, 10);
        exportCsv(`areas-${date}.csv`,
            ['Area', 'Total Invoices', 'Delivered', 'In Transit', 'Pending', 'Failed', 'Partial', 'Returned', 'Completion Rate'],
            rows.map(r => [r.area, r.total_invoices, r.delivered, r.in_transit, r.pending, r.failed, r.partial, r.returned, r.completion_rate])
        );
    } catch (e) {
        console.error('[Analytics] Export areas failed:', e.message);
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function exportManifests() {
    const btn = document.getElementById('manifests-export-btn');
    if (btn) btn.disabled = true;
    try {
        const qs   = buildParams({ page: 1, page_size: 200 });
        const res  = await apiFetch(`${API_BASE}/manifests?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const rows = data.items || [];
        const date = new Date().toISOString().slice(0, 10);
        const cap  = (data.total || 0) > 200;
        exportCsv(cap ? `manifests-top200-${date}.csv` : `manifests-${date}.csv`,
            ['Manifest #', 'Dispatch Date', 'Driver', 'Route', 'Total Invoices', 'Delivered', 'In Transit', 'Pending', 'Failed', 'Partial', 'Returned', 'Status'],
            rows.map(r => [r.manifest_number, r.dispatch_date, r.driver || 'Unassigned', r.route || '—', r.total_invoices, r.delivered, r.in_transit, r.pending, r.failed, r.partial, r.returned, r.manifest_status])
        );
    } catch (e) {
        console.error('[Analytics] Export manifests failed:', e.message);
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function exportExceptions() {
    const btn = document.getElementById('exceptions-export-btn');
    if (btn) btn.disabled = true;
    try {
        const qs   = buildParams({ page: 1, page_size: 200, status: exceptionsState.status });
        const res  = await apiFetch(`${API_BASE}/exceptions?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const rows = data.items || [];
        const date = new Date().toISOString().slice(0, 10);
        const cap  = (data.total || 0) > 200;
        exportCsv(cap ? `exceptions-top200-${date}.csv` : `exceptions-${date}.csv`,
            ['Invoice #', 'Manifest #', 'Dispatch Date', 'Customer', 'Area', 'Driver', 'Route', 'Status', 'Age (days)', 'PoD Present'],
            rows.map(r => [r.invoice_number, r.manifest_number, r.dispatch_date, r.customer_name, r.area, r.driver || 'Unassigned', r.route || '—', r.status, r.age_days, r.pod_present ? 'Yes' : 'No'])
        );
    } catch (e) {
        console.error('[Analytics] Export exceptions failed:', e.message);
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function exportDrivers() {
    const btn = document.getElementById('drivers-export-btn');
    if (btn) btn.disabled = true;
    try {
        const qs   = buildParams({ page: 1, page_size: 200 });
        const res  = await apiFetch(`${API_BASE}/drivers?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const rows = data.items || [];
        const date = new Date().toISOString().slice(0, 10);
        const cap  = (data.total || 0) > 200;
        exportCsv(cap ? `drivers-top200-${date}.csv` : `drivers-${date}.csv`,
            ['Driver', 'Manifests', 'Invoices', 'Delivered', 'Failed', 'Partial', 'Completion Rate', 'PoD Rate'],
            rows.map(r => [r.driver, r.manifests_assigned, r.total_invoices, r.delivered, r.failed, r.partial, r.success_rate, r.pod_compliance_rate])
        );
    } catch (e) {
        console.error('[Analytics] Export drivers failed:', e.message);
    } finally {
        if (btn) btn.disabled = false;
    }
}

// ── Invoiced by Date Range KPI ────────────────────────────────────────────

async function loadInvoicedByDateRange() {
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.innerHTML = val; };
    try {
        const res = await apiFetch(`${API_BASE}/invoiced-by-date-range?${buildParams()}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const d = await res.json();
        set('kpi-inv-by-date-count', fmtNum(sumAcrossCurrencies(d.totals_by_currency, 'invoice_count')));
        set('kpi-inv-by-date-value', formatTotalsByCurrency(d.totals_by_currency));
        set('kpi-inv-by-date-average', formatTotalsByCurrency(d.totals_by_currency, 'average_invoice_value'));
        set('kpi-inv-by-date-highest', formatTotalsByCurrency(d.totals_by_currency, 'highest_invoice_value'));
        set('kpi-inv-by-date-lowest', formatTotalsByCurrency(d.totals_by_currency, 'lowest_invoice_value'));
    } catch (e) {
        console.error('[Analytics] Invoiced by date range error:', e.message);
        set('kpi-inv-by-date-count', '—');
        set('kpi-inv-by-date-value', '—');
        set('kpi-inv-by-date-average', '—');
        set('kpi-inv-by-date-highest', '—');
        set('kpi-inv-by-date-lowest', '—');
    }
}

// ── Load Value Analysis ───────────────────────────────────────────────────

async function loadValueOverview() {
    try {
        const res = await apiFetch(`${API_BASE}/value-overview?${buildParams()}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        renderValueOverviewCards(await res.json());
    } catch (e) {
        console.error('[Analytics] Value overview error:', e.message);
        renderValueOverviewCards(null);
    }
}

function renderValueOverviewCards(d) {
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.innerHTML = val; };
    const totals = (d && Array.isArray(d.totals_by_currency)) ? d.totals_by_currency : [];
    if (!d || totals.length === 0) {
        ['kpi-total-value', 'kpi-value-manifest-count', 'kpi-avg-manifest-value',
         'kpi-highest-manifest-value', 'kpi-lowest-manifest-value'].forEach(id => set(id, '—'));
        return;
    }
    // totals_by_currency (Stage 4) — one line per currency, never blended.
    // Note: manifest_count is per-currency on the backend (a manifest mixing
    // USD+ZWL is counted under each currency it contains), so this sum can
    // exceed the true distinct manifest count for mixed-currency periods.
    set('kpi-total-value',            formatTotalsByCurrency(totals, 'total_value'));
    set('kpi-value-manifest-count',   fmtNum(sumAcrossCurrencies(totals, 'manifest_count')));
    set('kpi-avg-manifest-value',     formatTotalsByCurrency(totals, 'average_manifest_value'));
    set('kpi-highest-manifest-value', formatTotalsByCurrency(totals, 'highest_manifest_value'));
    set('kpi-lowest-manifest-value',  formatTotalsByCurrency(totals, 'lowest_manifest_value'));
}

async function loadValueManifests(offset) {
    if (offset !== undefined && offset !== null) valueManifestsState.offset = offset;
    showState('value-manifests', 'loading');

    try {
        const page = Math.floor(valueManifestsState.offset / valueManifestsState.limit) + 1;
        const qs   = buildParams({ page, page_size: valueManifestsState.limit });
        const res  = await apiFetch(`${API_BASE}/value-manifests?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        valueManifestsState.total = data.total || 0;

        const countEl = document.getElementById('value-manifests-count');
        if (countEl) countEl.textContent = `${valueManifestsState.total} manifests`;

        if (!data.items || data.items.length === 0) {
            showState('value-manifests', 'empty');
            updatePagination('value-manifests', valueManifestsState, loadValueManifests);
            return;
        }

        renderValueManifestsTable(data.items);
        updatePagination('value-manifests', valueManifestsState, loadValueManifests);
        showState('value-manifests', 'content');

    } catch (e) {
        console.error('[Analytics] Value manifests error:', e.message);
        const errEl = document.getElementById('value-manifests-error-msg');
        if (errEl) errEl.textContent = e.message;
        showState('value-manifests', 'error');
    }
}

function renderValueManifestsTable(rows) {
    const tbody = document.getElementById('value-manifests-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    rows.forEach(r => {
        // totals_by_currency (Stage 4) — a manifest mixing USD+ZWL shows one
        // line per currency, never a single blended figure.
        const totals = Array.isArray(r.totals_by_currency) ? r.totals_by_currency : [];
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(r.manifest_number)}</td>
            <td>${escapeHtml(r.dispatch_date || '—')}</td>
            <td>${escapeHtml(r.truck)}</td>
            <td>${escapeHtml(r.driver)}</td>
            <td>${escapeHtml(r.route)}</td>
            <td class="num-cell">${fmtNum(sumAcrossCurrencies(totals, 'invoice_count'))}</td>
            <td class="num-cell">${formatTotalsByCurrency(totals, 'total_value')}</td>
            <td class="num-cell">${formatTotalsByCurrency(totals, 'average_invoice_value')}</td>
        `;
        tbody.appendChild(tr);
    });
}

async function loadValueTrucks(offset) {
    if (offset !== undefined && offset !== null) valueTrucksState.offset = offset;
    showState('value-trucks', 'loading');

    try {
        const page = Math.floor(valueTrucksState.offset / valueTrucksState.limit) + 1;
        const qs   = buildParams({ page, page_size: valueTrucksState.limit });
        const res  = await apiFetch(`${API_BASE}/value-trucks?${qs}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        valueTrucksState.total = data.total || 0;

        if (!data.items || data.items.length === 0) {
            showState('value-trucks', 'empty');
            updatePagination('value-trucks', valueTrucksState, loadValueTrucks);
            return;
        }

        renderValueTrucksTable(data.items);
        updatePagination('value-trucks', valueTrucksState, loadValueTrucks);
        showState('value-trucks', 'content');

    } catch (e) {
        console.error('[Analytics] Value trucks error:', e.message);
        const errEl = document.getElementById('value-trucks-error-msg');
        if (errEl) errEl.textContent = e.message;
        showState('value-trucks', 'error');
    }
}

function renderValueTrucksTable(rows) {
    const tbody = document.getElementById('value-trucks-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    rows.forEach(r => {
        // totals_by_currency (Stage 4) — a truck carrying both USD and ZWL
        // loads shows one line per currency, never a single blended figure.
        // manifests_assigned / invoices_carried are true counts from the
        // backend (not inflated by the currency split).
        const totals = Array.isArray(r.totals_by_currency) ? r.totals_by_currency : [];
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(r.truck)}</td>
            <td class="num-cell">${fmtNum(r.manifests_assigned)}</td>
            <td class="num-cell">${fmtNum(r.invoices_carried)}</td>
            <td class="num-cell">${formatTotalsByCurrency(totals, 'total_value')}</td>
            <td class="num-cell">${formatTotalsByCurrency(totals, 'average_manifest_value')}</td>
            <td class="num-cell">${formatTotalsByCurrency(totals, 'highest_manifest_value')}</td>
            <td class="num-cell">${formatTotalsByCurrency(totals, 'lowest_manifest_value')}</td>
        `;
        tbody.appendChild(tr);
    });
}

// ── Load All Sections ─────────────────────────────────────────────────────

/**
 * Trigger all seven backend calls in parallel.
 * Each section manages its own state independently.
 */
function loadAll() {
    loadOverview();
    loadInvoicedByDateRange();
    loadValueOverview();
    loadTrends();
    loadManifests(0);
    loadDrivers(0);
    loadRoutes();
    loadExceptions(0);
    loadAging();
    loadValueManifests(0);
    loadValueTrucks(0);
}

// ── Filter Bar ────────────────────────────────────────────────────────────

let _searchTimer = null;

function readFilters() {
    filterState.dateFrom = document.getElementById('filter-date-from').value || '';
    filterState.dateTo   = document.getElementById('filter-date-to').value   || '';
    filterState.search   = document.getElementById('filter-search').value.trim();
    filterState.route    = document.getElementById('filter-route').value     || '';
}

function applyFilters() {
    readFilters();
    manifestsState.offset      = 0;
    driversState.offset        = 0;
    exceptionsState.offset     = 0;
    valueManifestsState.offset = 0;
    valueTrucksState.offset    = 0;
    loadAll();
}

function resetFilters() {
    document.getElementById('filter-date-from').value = '';
    document.getElementById('filter-date-to').value   = '';
    document.getElementById('filter-search').value    = '';
    document.getElementById('filter-route').value     = '';
    document.getElementById('filter-clear-search').classList.add('hidden');
    filterState = { dateFrom: '', dateTo: '', search: '', route: '' };
    exceptionsState.status = '';
    document.getElementById('exceptions-status-filter').value = '';
    manifestsState.offset      = 0;
    driversState.offset        = 0;
    exceptionsState.offset     = 0;
    valueManifestsState.offset = 0;
    valueTrucksState.offset    = 0;
    loadAll();
}

// ── Collapsible Sections ──────────────────────────────────────────────────

const COLLAPSE_KEY = 'analytics_section_state';

/**
 * Default collapsed state for each section.
 * true = collapsed on first visit; false = open.
 */
const SECTION_DEFAULTS = {
    'section-overview'   : false,
    'section-value'      : false,
    'section-trends'     : true,
    'section-manifests'  : false,
    'section-drivers'    : true,
    'section-routes'     : true,
    'section-exceptions' : false,
    'section-aging'      : false,
};

function _collapseReadState() {
    try { return JSON.parse(localStorage.getItem(COLLAPSE_KEY) || '{}'); }
    catch { return {}; }
}

function _collapseSaveState(state) {
    try { localStorage.setItem(COLLAPSE_KEY, JSON.stringify(state)); }
    catch { /* quota / private browsing — silently ignore */ }
}

/**
 * Apply saved (or default) collapse state before first render.
 * Called once on DOMContentLoaded, before loadAll().
 */
function applyCollapseState() {
    const saved = _collapseReadState();
    Object.keys(SECTION_DEFAULTS).forEach(id => {
        const section = document.getElementById(id);
        if (!section) return;
        const body = section.querySelector(':scope > .section-body');
        const icon = section.querySelector('.section-toggle-icon');
        const isCollapsed = Object.prototype.hasOwnProperty.call(saved, id)
            ? saved[id]
            : SECTION_DEFAULTS[id];
        if (isCollapsed) {
            if (body) body.classList.add('section-body--collapsed');
            section.classList.add('is-collapsed');
            if (icon) icon.classList.add('section-toggle-icon--collapsed');
        }
    });
}

/**
 * Attach click listeners to collapsible section headers.
 * Guards against clicks on interactive children (button, select, input, a).
 * Persists state to localStorage.
 */
function initCollapsible() {
    const saved = _collapseReadState();

    document.querySelectorAll('.collapsible-section > .section-header').forEach(header => {
        header.addEventListener('click', e => {
            // Do not toggle when clicking a control embedded in the header
            if (e.target.closest('button, select, input, a')) return;

            const section = header.closest('.collapsible-section');
            const body    = section && section.querySelector(':scope > .section-body');
            const icon    = section && section.querySelector('.section-toggle-icon');
            if (!section || !body) return;

            const isNowCollapsed = body.classList.toggle('section-body--collapsed');
            section.classList.toggle('is-collapsed', isNowCollapsed);
            if (icon) icon.classList.toggle('section-toggle-icon--collapsed', isNowCollapsed);

            if (section.id) {
                saved[section.id] = isNowCollapsed;
                _collapseSaveState(saved);
            }
        });
    });
}

// ── Initialisation ────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {

    // Auth checks — must run before any API call
    requireAuth();
    requireOfficeRole();
    document.body.classList.remove('auth-pending');

    // Populate route dropdown
    loadRouteFilter();

    // ── Filter bar ──────────────────────────────────────────────────────
    document.getElementById('filter-apply').addEventListener('click', applyFilters);
    document.getElementById('filter-reset').addEventListener('click', resetFilters);

    const searchEl   = document.getElementById('filter-search');
    const clearSearch = document.getElementById('filter-clear-search');

    searchEl.addEventListener('input', () => {
        clearSearch.classList.toggle('hidden', !searchEl.value);
        clearTimeout(_searchTimer);
        _searchTimer = setTimeout(applyFilters, 500);
    });

    clearSearch.addEventListener('click', () => {
        searchEl.value = '';
        clearSearch.classList.add('hidden');
        filterState.search         = '';
        manifestsState.offset      = 0;
        driversState.offset        = 0;
        exceptionsState.offset     = 0;
        valueManifestsState.offset = 0;
        valueTrucksState.offset    = 0;
        loadAll();
    });

    // ── Exceptions status filter ─────────────────────────────────────────
    document.getElementById('exceptions-status-filter').addEventListener('change', e => {
        exceptionsState.status = e.target.value;
        exceptionsState.offset = 0;
        loadExceptions(0);
    });

    // ── Trends granularity toggle ────────────────────────────────────────
    document.getElementById('trends-granularity').addEventListener('change', () => loadTrends());

    // ── Retry buttons ────────────────────────────────────────────────────
    document.getElementById('trends-retry').addEventListener('click',          () => loadTrends());
    document.getElementById('manifests-retry').addEventListener('click',       () => loadManifests());
    document.getElementById('drivers-retry').addEventListener('click',         () => loadDrivers());
    document.getElementById('routes-retry').addEventListener('click',          () => loadRoutes());
    document.getElementById('exceptions-retry').addEventListener('click',      () => loadExceptions());
    document.getElementById('aging-retry').addEventListener('click',           () => loadAging());
    document.getElementById('value-manifests-retry').addEventListener('click', () => loadValueManifests());
    document.getElementById('value-trucks-retry').addEventListener('click',    () => loadValueTrucks());

    // ── Export buttons ────────────────────────────────────────────────────
    document.getElementById('trends-export-btn').addEventListener('click',     () => exportTrends());
    document.getElementById('routes-export-btn').addEventListener('click',     () => exportRoutes());
    document.getElementById('areas-export-btn').addEventListener('click',      () => exportAreas());
    document.getElementById('manifests-export-btn').addEventListener('click',  () => exportManifests());
    document.getElementById('exceptions-export-btn').addEventListener('click', () => exportExceptions());
    document.getElementById('drivers-export-btn').addEventListener('click',    () => exportDrivers());

    // ── Page-size selectors ──────────────────────────────────────────────
    document.getElementById('manifests-page-size').addEventListener('change', e => {
        manifestsState.limit  = parseInt(e.target.value, 10);
        manifestsState.offset = 0;
        loadManifests(0);
    });

    document.getElementById('exceptions-page-size').addEventListener('change', e => {
        exceptionsState.limit  = parseInt(e.target.value, 10);
        exceptionsState.offset = 0;
        loadExceptions(0);
    });

    document.getElementById('drivers-page-size').addEventListener('change', e => {
        driversState.limit  = parseInt(e.target.value, 10);
        driversState.offset = 0;
        loadDrivers(0);
    });

    // ── Value page-size selectors ────────────────────────────────────────
    document.getElementById('value-manifests-page-size').addEventListener('change', e => {
        valueManifestsState.limit  = parseInt(e.target.value, 10);
        valueManifestsState.offset = 0;
        loadValueManifests(0);
    });

    document.getElementById('value-trucks-page-size').addEventListener('change', e => {
        valueTrucksState.limit  = parseInt(e.target.value, 10);
        valueTrucksState.offset = 0;
        loadValueTrucks(0);
    });

    // ── Collapsible sections ─────────────────────────────────────────────
    applyCollapseState();
    initCollapsible();

    // ── Initial data load ────────────────────────────────────────────────
    loadAll();
});

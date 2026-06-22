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

// Role constants — mirrors the backend VALID_ROLES set
const DS_ROLE_ADMIN        = 'ADMIN';
const DS_ROLE_DISPATCH     = 'DISPATCH';
const DS_ROLE_DRIVER       = 'DRIVER';
const DS_ROLE_REPORTS_ONLY = 'REPORTS_ONLY';

let dsUserRole = null;   // populated in dsInit() from localStorage

let dsState = {
    manifests: [],   // all manifests from last API load
    search:       '',
    dateFrom:     '',
    dateTo:       '',
    statusFilter: '',
    loading:      false,
};

// Populated by dsLoadMeta() on init from GET /api/delivery/meta.
// Replaces the old hardcoded DS_ALLOWED_TRANSITIONS constant.
let dsAllowedTransitions = {};
let dsBulkConfirmable    = [];

// ── Live tracking state ────────────────────────────────────────────────────
const DS_STALE_MINUTES = 5;     // age threshold before stale warning
const TRACKING_POLL_MS = 30000; // poll interval in ms

let dsTracking = {
    manifestNumber: null,   // manifest currently being tracked
    intervalId:     null,   // setInterval handle
    leafletMap:     null,   // Leaflet map instance
    leafletMarker:  null,   // Leaflet marker instance
    session:        0,      // incremented on each new tracking session; invalidates stale polls
};
let dsTrackHasData = false; // true once the map has been rendered with real position data
// ──────────────────────────────────────────────────────────────────────────

async function dsLoadMeta() {
    try {
        const res = await apiFetch(`${DS_API}/meta`);
        if (!res.ok) return;
        const data = await res.json();
        dsAllowedTransitions = data.allowed_transitions || {};
        dsBulkConfirmable = Object.entries(dsAllowedTransitions)
            .filter(([, allowed]) => allowed.includes('DELIVERED'))
            .map(([status]) => status);
    } catch (e) {
        console.error('[DeliveryStatus] Failed to load meta:', e);
    }
}

function dsCanConfirm() {
    return dsUserRole === DS_ROLE_ADMIN || dsUserRole === DS_ROLE_DISPATCH;
}

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

// recorded_at is a TIMESTAMPTZ string — format with time component for the tracking panel.
function dsFormatRecordedAt(val) {
    if (!val) return '—';
    try {
        const d = new Date(val);
        if (isNaN(d)) return String(val);
        return d.toLocaleString('en-GB', {
            day: '2-digit', month: 'short', year: 'numeric',
            hour: '2-digit', minute: '2-digit', second: '2-digit',
        });
    } catch { return String(val); }
}

function dsIsStale(recordedAt) {
    if (!recordedAt) return false;
    try {
        return (Date.now() - new Date(recordedAt).getTime()) > DS_STALE_MINUTES * 60000;
    } catch { return false; }
}

// Explicit allowlist — mirrors _OFFICE_READ_ROLES frozenset in deps.py exactly.
// A null/unknown dsUserRole is denied by default (Set.has returns false), which is
// safer than a blocklist where any unrecognised future role would be silently permitted.
const DS_TRACK_ROLES = new Set([DS_ROLE_ADMIN, DS_ROLE_DISPATCH, DS_ROLE_REPORTS_ONLY]);

function dsCanTrack() {
    return DS_TRACK_ROLES.has(dsUserRole);
}

function dsEsc(str) {
    const d = document.createElement('div');
    d.textContent = (str != null) ? String(str) : '';
    return d.innerHTML;
}

// ── Live tracking ─────────────────────────────────────────────────────────

/**
 * Open the tracking panel for the given manifest.
 * If the same manifest is already being tracked, just scroll to the panel.
 * Stops any previously active tracking before starting new.
 */
async function dsTrackManifest(manifestNumber) {
    if (dsTracking.manifestNumber === manifestNumber) {
        document.getElementById('ds-track-panel').scrollIntoView({ behavior: 'smooth' });
        return;
    }
    dsStopTracking();
    dsTracking.manifestNumber = manifestNumber;

    document.getElementById('ds-track-manifest-label').textContent = manifestNumber;
    document.getElementById('ds-track-panel').classList.remove('ds-track-panel--hidden');
    document.getElementById('ds-track-body').innerHTML = `
        <div class="ds-track-state-box">
            <div class="spinner" style="width:20px;height:20px;border-width:2px;flex-shrink:0"></div>
            <span>Fetching position&hellip;</span>
        </div>`;
    document.getElementById('ds-track-panel').scrollIntoView({ behavior: 'smooth', block: 'nearest' });

    const mySession = dsTracking.session;
    await dsPollTracking(mySession);
    if (dsTracking.session === mySession) {
        dsTracking.intervalId = setInterval(() => dsPollTracking(mySession), TRACKING_POLL_MS);
    }
}

/** Stop polling, destroy the Leaflet map, and hide the panel. */
function dsStopTracking() {
    if (dsTracking.intervalId)  { clearInterval(dsTracking.intervalId); dsTracking.intervalId = null; }
    if (dsTracking.leafletMap)  { dsTracking.leafletMap.remove(); dsTracking.leafletMap = null; dsTracking.leafletMarker = null; }
    dsTracking.manifestNumber = null;
    dsTracking.session++;
    dsTrackHasData = false;
    document.getElementById('ds-track-panel').classList.add('ds-track-panel--hidden');
    document.getElementById('ds-track-body').innerHTML = '';
}

/**
 * Fetch the latest ping for the tracked manifest and update the panel.
 * - 404 → no data yet; keeps retrying every 30 s.
 * - other error → shows error message if no map data; otherwise keeps last position.
 * - success → first call builds the map; subsequent calls update marker + metadata.
 */
async function dsPollTracking(session) {
    if (!dsTracking.manifestNumber) return;
    // Capture the manifest so the URL uses what was active when we started.
    const manifestNumber = dsTracking.manifestNumber;
    try {
        const res = await apiFetch(
            `/api/tracking/latest/${encodeURIComponent(manifestNumber)}`
        );
        // Discard result if the session was closed or superseded while we awaited.
        if (dsTracking.session !== session) return;
        if (res.status === 404) {
            if (!dsTrackHasData) dsSetTrackBodyState('no-data');
            return;
        }
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (dsTracking.session !== session) return;
        if (!dsTrackHasData) {
            dsInitTrackBody(data);   // sets dsTrackHasData internally after metadata is in DOM
        } else {
            dsUpdateTrackMeta(data);
        }
    } catch (e) {
        console.error('[Tracking] Poll error:', e);
        if (dsTracking.session !== session) return;
        // Only wipe the panel body if we have never shown position data.
        // If a map is already visible, silently keep the last known position.
        if (!dsTrackHasData) dsSetTrackBodyState('error', e.message);
    }
}

/**
 * First-render: build metadata rows, then attempt Leaflet map initialization.
 *
 * dsTrackHasData is set to true immediately after innerHTML is written so that:
 *   - driver/reg/timestamp/coordinates are always visible even if Leaflet is missing
 *   - a Leaflet init failure does NOT trigger dsSetTrackBodyState('error') on the
 *     next poll, which would wipe the metadata panel
 * If Leaflet is unavailable (L undefined) or map init throws, dsShowMapUnavailable()
 * renders a plain fallback inside the map container; all metadata rows remain intact.
 */
function dsInitTrackBody(data) {
    const { latitude: lat, longitude: lng, recorded_at: recordedAt,
            driver_username: driver, reg_number: reg, accuracy } = data;
    const stale = dsIsStale(recordedAt);

    document.getElementById('ds-track-body').innerHTML = `
        <div id="ds-track-stale" class="ds-track-stale" style="${stale ? '' : 'display:none'}">
            <i data-lucide="alert-triangle" style="width:15px;height:15px;flex-shrink:0"></i>
            Last update was more than ${DS_STALE_MINUTES} minutes ago &mdash; truck may have stopped reporting.
        </div>
        <div class="ds-track-meta">
            <div class="ds-track-meta-item">
                <span class="ds-track-meta-label">Driver</span>
                <span class="ds-track-meta-value">${dsEsc(driver)}</span>
            </div>
            <div class="ds-track-meta-item">
                <span class="ds-track-meta-label">Truck Reg</span>
                <span class="ds-track-meta-value">${dsEsc(reg)}</span>
            </div>
            <div class="ds-track-meta-item">
                <span class="ds-track-meta-label">Last Update</span>
                <span class="ds-track-meta-value" id="ds-track-recorded-at">${dsFormatRecordedAt(recordedAt)}</span>
            </div>
            <div class="ds-track-meta-item">
                <span class="ds-track-meta-label">Coordinates</span>
                <span class="ds-track-meta-value" id="ds-track-coords">${lat.toFixed(6)}, ${lng.toFixed(6)}</span>
            </div>
            ${accuracy != null
                ? `<div class="ds-track-meta-item">
                       <span class="ds-track-meta-label">Accuracy</span>
                       <span class="ds-track-meta-value" id="ds-track-accuracy">\u00b1${Math.round(accuracy)}\u00a0m</span>
                   </div>`
                : ''}
        </div>
        <div id="ds-track-map"></div>`;
    lucide.createIcons();

    // Metadata is now in the DOM — mark as having data so no subsequent poll failure
    // can wipe it via dsSetTrackBodyState().
    dsTrackHasData = true;

    if (typeof L === 'undefined') {
        dsShowMapUnavailable();
        return;
    }

    try {
        const map = L.map(document.getElementById('ds-track-map')).setView([lat, lng], 14);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '\u00a9 <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            maxZoom: 19,
        }).addTo(map);
        const marker = L.marker([lat, lng]).addTo(map);
        marker.bindPopup(
            `<b>${dsEsc(driver)}</b> \u2014 ${dsEsc(reg)}<br>${dsFormatRecordedAt(recordedAt)}`
        ).openPopup();
        dsTracking.leafletMap    = map;
        dsTracking.leafletMarker = marker;
    } catch (e) {
        console.error('[Tracking] Leaflet map init failed:', e);
        dsShowMapUnavailable();
    }
}

/** Replace the map container with a plain-text fallback when the map cannot render. */
function dsShowMapUnavailable() {
    const mapEl = document.getElementById('ds-track-map');
    if (!mapEl) return;
    mapEl.style.height = 'auto';
    mapEl.innerHTML = `<div class="ds-track-state-box" style="justify-content:center">
        <i data-lucide="map-off"></i>
        <span>Map unavailable &mdash; use the coordinates above to locate the truck.</span>
    </div>`;
    lucide.createIcons();
}

/**
 * Subsequent-poll update: move the marker and refresh metadata text in place.
 * Does NOT re-render the DOM — preserves the user's map zoom and pan.
 */
function dsUpdateTrackMeta(data) {
    const { latitude: lat, longitude: lng, recorded_at: recordedAt, accuracy } = data;

    const raEl = document.getElementById('ds-track-recorded-at');
    if (raEl) raEl.textContent = dsFormatRecordedAt(recordedAt);

    const coordsEl = document.getElementById('ds-track-coords');
    if (coordsEl) coordsEl.textContent = `${lat.toFixed(6)}, ${lng.toFixed(6)}`;

    const accEl = document.getElementById('ds-track-accuracy');
    if (accEl && accuracy != null) accEl.textContent = `\u00b1${Math.round(accuracy)}\u00a0m`;

    const staleEl = document.getElementById('ds-track-stale');
    if (staleEl) staleEl.style.display = dsIsStale(recordedAt) ? '' : 'none';

    // Map is optional — update marker only when Leaflet initialised successfully.
    if (dsTracking.leafletMap && dsTracking.leafletMarker) {
        const latlng = L.latLng(lat, lng);
        dsTracking.leafletMarker.setLatLng(latlng);
        dsTracking.leafletMap.panTo(latlng);
        dsTracking.leafletMarker.getPopup().setContent(
            `<b>${dsEsc(data.driver_username)}</b> \u2014 ${dsEsc(data.reg_number)}<br>${dsFormatRecordedAt(recordedAt)}`
        );
    }
}

/** Render a non-map state (no-data or error) inside the tracking panel body. */
function dsSetTrackBodyState(which, msg) {
    const html = (which === 'no-data')
        ? `<div class="ds-track-state-box">
               <i data-lucide="map-pin-off"></i>
               <span>No location data received yet for this manifest. Retrying every 30 s.</span>
           </div>`
        : `<div class="ds-track-state-box">
               <i data-lucide="wifi-off"></i>
               <span>Could not fetch location${msg ? ': ' + dsEsc(msg) : ''}. Retrying in 30 s.</span>
           </div>`;
    document.getElementById('ds-track-body').innerHTML = html;
    lucide.createIcons();
}

// ── Load manifests from API ────────────────────────────────────────────────

async function loadManifests() {
    if (dsState.loading) return;
    dsState.loading = true;
    showDsState('loading');

    try {
        const params = new URLSearchParams();
        if (dsState.dateFrom) params.append('date_from', dsState.dateFrom);
        if (dsState.dateTo)   params.append('date_to',   dsState.dateTo);
        // statusFilter is applied client-side in filterManifests() — backend has no status param

        const res = await apiFetch(`${DS_API}/manifests?${params}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);

        const data = await res.json();
        dsState.manifests = data.manifests || [];
        renderManifests(filterManifests(dsState.manifests));

    } catch (e) {
        console.error('[DeliveryStatus] Error loading manifests:', e);
        const msgEl = document.getElementById('ds-error-msg');
        if (msgEl) msgEl.textContent = e.message;
        showDsState('error');
    } finally {
        dsState.loading = false;
    }
}

// ── Client-side filters (search + manifest status) ─────────────────────────

function filterManifests(manifests) {
    let result = manifests;

    const q = dsState.search.toLowerCase().trim();
    if (q) {
        result = result.filter(m =>
            (m.manifest_number      || '').toLowerCase().includes(q) ||
            (m.driver               || '').toLowerCase().includes(q) ||
            (m.reg_number           || '').toLowerCase().includes(q) ||
            (m.consignment_number   || '').toLowerCase().includes(q)
        );
    }

    if (dsState.statusFilter) {
        result = result.filter(m =>
            (m.delivery_summary?.status || 'PENDING') === dsState.statusFilter
        );
    }

    return result;
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

        const isSwift = m.delivery_type === 'SWIFT_3PL';
        const swiftBadgeHtml = isSwift
            ? `<br><span style="font-size:0.7rem;font-weight:600;color:#92400e;">
                   Swift / 3PL: ${dsEsc(m.third_party_provider || 'Swift')} - ${dsEsc(m.consignment_number || '—')}
               </span>`
            : '';

        row.innerHTML = `
            <td><strong>${dsEsc(m.manifest_number)}</strong>${swiftBadgeHtml}</td>
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
            <td style="white-space:nowrap">
                <button class="report-btn report-btn-secondary ds-detail-btn"
                        data-manifest="${dsEsc(m.manifest_number)}">
                    <i data-lucide="eye"></i> Details
                </button>
                ${dsCanTrack() && (s.status || 'PENDING') === 'IN_PROGRESS'
                    ? `<button class="report-btn report-btn-primary ds-track-btn"
                               data-manifest="${dsEsc(m.manifest_number)}"
                               style="margin-left:6px">
                           <i data-lucide="map-pin"></i> Track
                       </button>`
                    : ''}
            </td>
        `;

        // Row click (not on button — button has its own listener via event delegation)
        row.addEventListener('click', (e) => {
            if (e.target.closest('.ds-detail-btn') || e.target.closest('.ds-track-btn')) return;
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

// Returns <option> elements for the allowed next statuses from currentStatus.
// Returns empty string for terminal statuses (caller renders "—" in that case).
function renderActionOptions(currentStatus) {
    const allowed = dsAllowedTransitions[currentStatus] || [];
    const labels  = {
        'PENDING':    'Pending',
        'IN_TRANSIT': 'In Transit',
        'DELIVERED':  'Delivered',
        'FAILED':     'Failed',
        'RETURNED':   'Returned',
        'PARTIAL':    'Partial',
    };
    return allowed.map((s, i) =>
        `<option value="${s}"${i === 0 ? ' selected' : ''}>${labels[s] || s}</option>`
    ).join('');
}


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
    const body       = document.getElementById('ds-modal-body');
    const items      = manifest.items || [];
    const canConfirm = dsCanConfirm();
    const colCount   = canConfirm ? 8 : 7;

    const rowsHtml = items.length
        ? items.map(item => {
            const status = item.delivery_status || 'PENDING';
            const nextOptions = renderActionOptions(status);
            const actionCell = canConfirm
                ? nextOptions
                    ? `<div style="display:flex;gap:6px;align-items:center;">
                           <select class="ds-status-select"
                                   data-item-id="${item.report_item_id}"
                                   style="font-size:0.75rem;padding:2px 4px;border:1px solid #e2e8f0;border-radius:4px;cursor:pointer;">
                               ${nextOptions}
                           </select>
                           <button class="apply-invoice-action report-btn report-btn-primary"
                                   data-item-id="${item.report_item_id}"
                                   data-invoice="${dsEsc(item.invoice_number)}"
                                   data-manifest="${dsEsc(manifest.manifest_number)}"
                                   style="font-size:0.75rem;padding:3px 10px;white-space:nowrap;">
                               Apply
                           </button>
                       </div>`
                    : `<span style="font-size:0.75rem;color:#94a3b8;font-style:italic;">—</span>`
                : '';
            const modeBadge = item.delivery_mode === 'THIRD_PARTY'
                ? ` <span style="font-size:0.68rem;font-weight:600;background:#fef3c7;color:#92400e;border:1px solid #fcd34d;border-radius:3px;padding:1px 5px;vertical-align:middle;" title="Third Party delivery">3P</span>`
                : '';
            return `
            <tr>
                <td>${dsEsc(item.invoice_number || '—')}</td>
                <td>${dsEsc(item.customer_name  || '—')}${modeBadge}</td>
                <td>${dsEsc(item.area           || '—')}</td>
                <td>${invoiceStatusBadge(status)}</td>
                <td class="ds-notes">${dsEsc(item.notes || '—')}</td>
                <td>${item.has_pod && item.pod_image_path
                    ? `<button class="ds-pod-btn report-btn report-btn-secondary"
                               data-pod-path="${dsEsc(item.pod_image_path)}"
                               style="font-size:0.75rem;padding:3px 10px;">
                           View PoD
                       </button>`
                    : '—'
                }</td>
                <td>${item.updated_at ? dsFormatDate(item.updated_at) : '—'}</td>
                ${canConfirm ? `<td style="white-space:nowrap">${actionCell}</td>` : ''}
            </tr>`;
        }).join('')
        : `<tr><td colspan="${colCount}" style="text-align:center;color:#94a3b8">No invoices found</td></tr>`;

    // Bulk action bar — only rendered for DISPATCH/ADMIN.
    // The dropdown always shows all three targets; eligibility feedback is delivered
    // via the success/zero-update message after the call, not by pre-filtering options.
    const bulkActionHtml = canConfirm
        ? `<div style="display:flex;align-items:center;gap:0.75rem;flex-wrap:wrap;
                       margin-bottom:12px;padding:0.6rem 0.9rem;
                       background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;">
               <span style="font-size:0.75rem;font-weight:600;color:#64748b;white-space:nowrap;">
                   Bulk Action:
               </span>
               <select class="ds-bulk-target-select"
                       style="font-size:0.8rem;padding:3px 8px;border:1px solid #cbd5e1;
                              border-radius:6px;cursor:pointer;flex:1;min-width:220px;max-width:280px;">
                   <option value="">— Select action —</option>
                   <option value="IN_TRANSIT">Mark Entire Manifest In Transit</option>
                   <option value="DELIVERED">Mark Entire Manifest Delivered</option>
                   <option value="RETURNED">Mark Entire Manifest Returned</option>
                   <option value="FAILED">Mark Entire Manifest Failed</option>
               </select>
               <button class="ds-bulk-apply-btn report-btn report-btn-primary"
                       data-manifest="${dsEsc(manifest.manifest_number)}"
                       style="font-size:0.8rem;padding:4px 14px;white-space:nowrap;">
                   <i data-lucide="check-square"></i> Apply to All
               </button>
           </div>`
        : '';

    const isSwiftManifest = manifest.delivery_type === 'SWIFT_3PL';
    const swiftInfoHtml = isSwiftManifest
        ? `<span><strong>Delivery Type:</strong> Swift / 3PL</span>
           <span><strong>Provider:</strong> ${dsEsc(manifest.third_party_provider || 'Swift')}</span>
           <span><strong>Consignment No.:</strong> ${dsEsc(manifest.consignment_number || '—')}</span>
           <span><strong>Consignment Date:</strong> ${manifest.consignment_date ? dsFormatDate(manifest.consignment_date) : '—'}</span>`
        : '';

    body.innerHTML = `
        <div class="ds-modal-info">
            <span><strong>Driver:</strong> ${dsEsc(manifest.driver || '—')}</span>
            <span><strong>Date:</strong> ${dsFormatDate(manifest.date_dispatched)}</span>
            <span><strong>Overall Status:</strong> ${manifestStatusBadge(manifest.manifest_status || 'PENDING')}</span>
            <span><strong>Invoices:</strong> ${items.length}</span>
            ${swiftInfoHtml}
        </div>
        ${bulkActionHtml}
        <div class="table-container ds-detail-table">
            <table>
                <thead>
                    <tr>
                        <th>Invoice #</th>
                        <th>Customer</th>
                        <th>Area</th>
                        <th>Delivery Status</th>
                        <th>Notes</th>
                        <th>PoD</th>
                        <th>Last Updated</th>
                        ${canConfirm ? '<th>Actions</th>' : ''}
                    </tr>
                </thead>
                <tbody>${rowsHtml}</tbody>
            </table>
        </div>`;
    lucide.createIcons();
}

/**
 * POST /api/delivery/manifests/{manifestNumber}/bulk-status-update
 *
 * Asks the backend to update all eligible invoices in the manifest to
 * targetStatus in one atomic operation.  Eligibility is determined server-side
 * from ALLOWED_TRANSITIONS — PENDING items and already-resolved items are
 * skipped automatically.  Shows a confirmation before submitting and an
 * outcome alert (with updated/skipped counts) on success.
 */
async function bulkUpdateManifest(manifestNumber, targetStatus) {
    if (!targetStatus) {
        alert('Please select a bulk action before clicking Apply.');
        return;
    }

    const labels = { IN_TRANSIT: 'In Transit', DELIVERED: 'Delivered', RETURNED: 'Returned', FAILED: 'Failed' };
    const label  = labels[targetStatus] || targetStatus;

    const confirmDetail = targetStatus === 'IN_TRANSIT'
        ? 'Only pending invoices will be updated. Failed, partial, returned, and already in-transit invoices will be skipped.'
        : 'Only invoices currently in an eligible state will be updated. PENDING and already-resolved invoices will be skipped.';

    if (!confirm(
        `Mark all eligible invoices in manifest ${manifestNumber} as ${label}?\n\n` +
        confirmDetail
    )) return;

    const applyBtn = document.querySelector('.ds-bulk-apply-btn');
    if (applyBtn) { applyBtn.disabled = true; applyBtn.textContent = 'Saving\u2026'; }

    try {
        const res = await apiFetch(
            `${DS_API}/manifests/${encodeURIComponent(manifestNumber)}/bulk-status-update`,
            { method: 'POST', body: JSON.stringify({ target_status: targetStatus }) }
        );

        if (res.ok) {
            const data = await res.json();
            const inv  = (n) => `${n} invoice${n !== 1 ? 's' : ''}`;
            const msg  = data.updated > 0
                ? `${inv(data.updated)} marked as ${label}.` +
                  (data.skipped > 0 ? ` ${inv(data.skipped)} skipped (ineligible or already resolved).` : '')
                : `No invoices were updated — all ${inv(data.skipped)} are ineligible or already resolved.`;
            alert(msg);
            openDetail(manifestNumber);
            loadManifests();
        } else {
            const err = await res.json().catch(() => ({}));
            alert(err.detail || 'Failed to apply bulk action.');
            if (applyBtn) {
                applyBtn.disabled = false;
                applyBtn.innerHTML = '<i data-lucide="check-square"></i> Apply to All';
                lucide.createIcons();
            }
        }
    } catch (e) {
        console.error('[DeliveryStatus] bulkUpdateManifest error:', e);
        alert('Server error applying bulk action.');
        if (applyBtn) {
            applyBtn.disabled = false;
            applyBtn.innerHTML = '<i data-lucide="check-square"></i> Apply to All';
            lucide.createIcons();
        }
    }
}

async function viewPodFile(path) {
    try {
        const res = await apiFetch(`/api/delivery/files/${path}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const blob = await res.blob();
        const url  = URL.createObjectURL(blob);
        window.open(url, '_blank');
        // Release the object URL after 60 s — long enough for the tab to load
        setTimeout(() => URL.revokeObjectURL(url), 60000);
    } catch (e) {
        alert(`Could not load PoD file: ${e.message}`);
    }
}

async function applyInvoiceAction(reportItemId, invoiceNumber, manifestNumber, newStatus) {
    if (!confirm(`Set invoice ${invoiceNumber} to ${newStatus}?`)) return;

    const applyBtn = document.querySelector(`.apply-invoice-action[data-item-id="${reportItemId}"]`);
    const select   = document.querySelector(`.ds-status-select[data-item-id="${reportItemId}"]`);

    // Disable controls immediately to prevent duplicate submissions
    if (applyBtn) { applyBtn.disabled = true; applyBtn.textContent = 'Saving…'; }
    if (select)   { select.disabled = true; }

    try {
        const res = await apiFetch(`${DS_API}/updates/${reportItemId}`, {
            method: 'PUT',
            body: JSON.stringify({ status: newStatus }),
        });

        if (res.ok) {
            // Re-fetch detail and list so all counts/badges update
            openDetail(manifestNumber);
            loadManifests();
        } else {
            const err = await res.json().catch(() => ({}));
            alert(err.detail || 'Failed to update delivery status.');
            if (applyBtn) { applyBtn.disabled = false; applyBtn.textContent = 'Apply'; }
            if (select)   { select.disabled = false; }
        }
    } catch (e) {
        console.error('[DeliveryStatus] applyInvoiceAction error:', e);
        alert('Server error updating delivery status.');
        if (applyBtn) { applyBtn.disabled = false; applyBtn.textContent = 'Apply'; }
        if (select)   { select.disabled = false; }
    }
}

async function bulkConfirmManifest(manifestNumber) {
    if (!confirm(
        `Mark ALL unresolved invoices in manifest ${manifestNumber} as Delivered?\n\n` +
        `Already-resolved invoices (Failed, Partial, Returned, Delivered) will not be changed.`
    )) return;

    const btn = document.querySelector(`.ds-bulk-confirm-btn[data-manifest="${manifestNumber}"]`);
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = 'Saving…';
    }

    try {
        const res = await apiFetch(
            `${DS_API}/manifests/${encodeURIComponent(manifestNumber)}/bulk-confirm`,
            { method: 'POST' }
        );

        if (res.ok) {
            const data = await res.json();
            // Re-fetch detail and list so everything reflects the new statuses
            openDetail(manifestNumber);
            loadManifests();
            // Brief success notice in the button area (detail will re-render immediately)
            console.info(
                `[DeliveryStatus] Bulk confirm: manifest=${manifestNumber} ` +
                `updated=${data.updated} skipped=${data.skipped}`
            );
        } else {
            const err = await res.json().catch(() => ({}));
            alert(err.detail || 'Failed to bulk-confirm manifest.');
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '<i data-lucide="check-circle-2"></i> Confirm Entire Manifest Delivered';
                lucide.createIcons();
            }
        }
    } catch (e) {
        console.error('[DeliveryStatus] bulkConfirmManifest error:', e);
        alert('Server error confirming manifest.');
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i data-lucide="check-circle-2"></i> Confirm Entire Manifest Delivered';
            lucide.createIcons();
        }
    }
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
    renderManifests(filterManifests(dsState.manifests));
}

// ── Init ───────────────────────────────────────────────────────────────────

function dsInit() {
    requireAuth();
    dsUserRole = getUserRole();

    document.getElementById('ds-apply-btn').addEventListener('click', applyFilters);
    document.getElementById('ds-reset-btn').addEventListener('click', resetFilters);
    document.getElementById('ds-search').addEventListener('input', handleSearch);
    document.getElementById('ds-close-modal-btn').addEventListener('click', closeDetail);
    document.getElementById('ds-back-btn').addEventListener('click',
        () => { dsStopTracking(); window.location.href = 'index.html'; });

    // Event delegation for Detail and Track buttons inside the manifest table
    document.getElementById('ds-table-body').addEventListener('click', (e) => {
        const detailBtn = e.target.closest('.ds-detail-btn');
        if (detailBtn) { openDetail(detailBtn.dataset.manifest); return; }
        const trackBtn = e.target.closest('.ds-track-btn');
        if (trackBtn) dsTrackManifest(trackBtn.dataset.manifest);
    });

    document.getElementById('ds-track-close-btn').addEventListener('click', dsStopTracking);

    // Close modal on backdrop click; handle PoD view and Mark Delivered buttons inside modal
    document.getElementById('ds-detail-modal').addEventListener('click', (e) => {
        if (e.target === document.getElementById('ds-detail-modal')) {
            closeDetail();
            return;
        }
        const podBtn = e.target.closest('.ds-pod-btn');
        if (podBtn) viewPodFile(podBtn.dataset.podPath);

        const applyBtn = e.target.closest('.apply-invoice-action');
        if (applyBtn) {
            const sel = document.querySelector(`.ds-status-select[data-item-id="${applyBtn.dataset.itemId}"]`);
            if (sel) {
                applyInvoiceAction(
                    parseInt(applyBtn.dataset.itemId, 10),
                    applyBtn.dataset.invoice,
                    applyBtn.dataset.manifest,
                    sel.value,
                );
            }
        }

        const bulkBtn = e.target.closest('.ds-bulk-confirm-btn');
        if (bulkBtn) bulkConfirmManifest(bulkBtn.dataset.manifest);

        const bulkApplyBtn = e.target.closest('.ds-bulk-apply-btn');
        if (bulkApplyBtn) {
            const sel = document.querySelector('.ds-bulk-target-select');
            bulkUpdateManifest(bulkApplyBtn.dataset.manifest, sel ? sel.value : '');
        }
    });

    // Load backend constants (allowed_transitions) then fetch manifests.
    // Both calls are async; meta will resolve well before the user opens a detail modal.
    dsLoadMeta();
    loadManifests();
}

document.addEventListener('DOMContentLoaded', dsInit);

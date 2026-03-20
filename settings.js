/**
 * settings.js — Settings page logic
 *
 * Self-contained: does not depend on script.js.
 * Loaded only by settings.html.
 * Requires auth.js to be loaded first.
 */

// ── API ───────────────────────────────────────────────────
const API_URL = '/api';

// ── Role constants ────────────────────────────────────────
const ROLE_ADMIN        = 'ADMIN';
const ROLE_DISPATCH     = 'DISPATCH';
const ROLE_REPORTS_ONLY = 'REPORTS_ONLY';
const ROLE_DRIVER       = 'DRIVER';

// ── Settings data ─────────────────────────────────────────
let settingsData = {
    drivers: [],
    assistants: [],
    checkers: [],
    routes: [],
    trucks: [],
    customerRoutes: []
};

// ── Password reset state ──────────────────────────────────
let resetPasswordUserId = null;

// ── No-op save (kept for saveCustomerRouteEdit compat) ────
function saveSettings() {}

// ─────────────────────────────────────────────────────────
// DATA LOADING
// ─────────────────────────────────────────────────────────

async function loadSettings() {
    async function safeFetch(url, label) {
        try {
            const res = await apiFetch(url);
            if (!res.ok) {
                console.error(`[Settings] ${label} returned HTTP ${res.status}`);
                return null;
            }
            return await res.json();
        } catch (err) {
            console.error(`[Settings] ${label} failed: ${err.message}`);
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

    settingsData.drivers        = driversData?.values    ?? [];
    settingsData.assistants     = assistantsData?.values ?? [];
    settingsData.checkers       = checkersData?.values   ?? [];
    settingsData.routes         = routesData?.values     ?? [];
    settingsData.trucks         = trucksData?.trucks     ?? [];
    settingsData.customerRoutes = custRoutesData?.routes ?? [];

    console.log('[Settings] Settings loaded.');
}

// ─────────────────────────────────────────────────────────
// TAB SWITCHING
// ─────────────────────────────────────────────────────────

function handleSettingsTabClick(e) {
    const button = e.target.closest('.settings-tab');
    if (!button) return;

    const tabName = button.dataset.tab;
    if (!tabName) return;

    document.querySelectorAll('.settings-tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.tab === tabName);
    });

    document.querySelectorAll('.settings-tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `tab-${tabName}`);
    });
}

// ─────────────────────────────────────────────────────────
// DROPDOWNS
// ─────────────────────────────────────────────────────────

function populateTruckFormDropdowns() {
    const driverSelect = document.getElementById('new-truck-driver');
    if (driverSelect) {
        driverSelect.innerHTML = '<option value="">Default Driver (optional)</option>';
        settingsData.drivers.forEach(name => {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name;
            driverSelect.appendChild(opt);
        });
    }

    const assistantSelect = document.getElementById('new-truck-assistant');
    if (assistantSelect) {
        assistantSelect.innerHTML = '<option value="">Default Assistant (optional)</option>';
        settingsData.assistants.forEach(name => {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name;
            assistantSelect.appendChild(opt);
        });
    }

    const checkerSelect = document.getElementById('new-truck-checker');
    if (checkerSelect) {
        checkerSelect.innerHTML = '<option value="">Default Checker (optional)</option>';
        settingsData.checkers.forEach(name => {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name;
            checkerSelect.appendChild(opt);
        });
    }
}

function populateCustomerRouteDropdown() {
    const select = document.getElementById('new-customer-route');
    if (!select) return;
    select.innerHTML = '<option value="">Select Route</option>';
    settingsData.routes.forEach(route => {
        const opt = document.createElement('option');
        opt.value = route;
        opt.textContent = route;
        select.appendChild(opt);
    });
}

async function populateCustomerSuggestions() {
    const dataList = document.getElementById('customer-suggestions');
    if (!dataList) return;

    dataList.innerHTML = '';
    const customers = new Set();

    try {
        const response = await apiFetch(`${API_URL}/customers`);
        if (response.ok) {
            const data = await response.json();
            data.customers.forEach(name => customers.add(name));
        }
    } catch (error) {
        console.warn('Could not fetch customers:', error);
    }

    if (settingsData.customerRoutes) {
        settingsData.customerRoutes.forEach(entry => customers.add(entry.customer_name));
    }

    Array.from(customers).sort().forEach(customer => {
        const opt = document.createElement('option');
        opt.value = customer;
        dataList.appendChild(opt);
    });
}

// ─────────────────────────────────────────────────────────
// SETTINGS CRUD (drivers / assistants / checkers / routes)
// ─────────────────────────────────────────────────────────

function renderSettingsList(category) {
    const listEl = document.getElementById(`${category}-list`);
    if (!listEl) return;

    const items = settingsData[category] || [];

    if (items.length === 0) {
        listEl.innerHTML = '<div class="settings-empty">No items added yet</div>';
        return;
    }

    let html = '';
    items.forEach((item, index) => {
        html += `
        <div class="settings-item">
            <div class="settings-item-info">
                <span class="settings-item-name">${item}</span>
            </div>
            <div class="settings-item-actions">
                <button class="btn-icon btn-edit" onclick="editSettingsItem('${category}', ${index})" title="Edit">
                    <i data-lucide="pencil"></i>
                </button>
                <button class="btn-icon btn-delete" onclick="removeSettingsItem('${category}', ${index})" title="Delete">
                    <i data-lucide="trash-2"></i>
                </button>
            </div>
        </div>
        `;
    });

    listEl.innerHTML = html;
    lucide.createIcons();
}

function editSettingsItem(category, index) {
    const currentValue = settingsData[category][index];
    const listEl = document.getElementById(`${category}-list`);
    const itemEl = listEl.querySelectorAll('.settings-item')[index];

    itemEl.innerHTML = `
        <div class="settings-edit-form">
            <input type="text" id="edit-setting-${category}-${index}" value="${currentValue.replace(/"/g, '&quot;')}" style="flex:1;">
            <div class="settings-edit-actions">
                <button class="btn btn-primary btn-sm" onclick="saveSettingsEdit('${category}', ${index})">
                    <i data-lucide="check"></i> Save
                </button>
                <button class="btn btn-secondary btn-sm" onclick="cancelSettingsEdit('${category}')">
                    <i data-lucide="x"></i> Cancel
                </button>
            </div>
        </div>
    `;

    lucide.createIcons();
    const input = document.getElementById(`edit-setting-${category}-${index}`);
    input.focus();
    input.select();
}

async function saveSettingsEdit(category, index) {
    const oldValue = settingsData[category][index];
    const input = document.getElementById(`edit-setting-${category}-${index}`);
    const newValue = input.value.trim();

    if (!newValue) {
        alert('Name cannot be empty.');
        input.focus();
        return;
    }

    if (newValue === oldValue) {
        renderSettingsList(category);
        return;
    }

    try {
        const response = await apiFetch(`${API_URL}/settings`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ category, old_value: oldValue, new_value: newValue })
        });

        if (response.ok) {
            settingsData[category][index] = newValue;
            renderSettingsList(category);
            if (['drivers', 'assistants', 'checkers'].includes(category)) {
                populateTruckFormDropdowns();
            }
        } else {
            const err = await response.json().catch(() => ({}));
            alert(err.detail || 'Failed to update. The name may already exist.');
        }
    } catch (e) {
        console.error(e);
        alert('Server error updating setting.');
    }
}

function cancelSettingsEdit(category) {
    renderSettingsList(category);
}

async function addSettingsItem(category) {
    const inputMap = {
        drivers:    'new-driver-name',
        assistants: 'new-assistant-name',
        checkers:   'new-checker-name',
        routes:     'new-route-name'
    };

    const input = document.getElementById(inputMap[category]);
    if (!input) return;

    const value = input.value.trim();
    if (!value) {
        alert('Please enter a name.');
        return;
    }

    try {
        const response = await apiFetch(`${API_URL}/settings`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ category, value })
        });

        if (response.ok) {
            if (!settingsData[category]) settingsData[category] = [];
            settingsData[category].push(value);
            input.value = '';
            renderSettingsList(category);
            if (['drivers', 'assistants', 'checkers'].includes(category)) {
                populateTruckFormDropdowns();
            }
            // If routes changed, refresh customer route dropdown too
            if (category === 'routes') {
                populateCustomerRouteDropdown();
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

    if (!confirm(`Are you sure you want to remove "${itemName}"?`)) return;

    try {
        const response = await apiFetch(`${API_URL}/settings/${category}/${encodeURIComponent(itemName)}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            settingsData[category].splice(index, 1);
            renderSettingsList(category);
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

// ─────────────────────────────────────────────────────────
// TRUCKS
// ─────────────────────────────────────────────────────────

function renderTrucksList() {
    const listEl = document.getElementById('trucks-list');

    if (settingsData.trucks.length === 0) {
        listEl.innerHTML = '<div class="settings-empty">No trucks added yet</div>';
        return;
    }

    listEl.innerHTML = settingsData.trucks.map((truck, index) => {
        const details = [];
        if (truck.driver)    details.push(`Driver: ${truck.driver}`);
        if (truck.assistant) details.push(`Assistant: ${truck.assistant}`);
        if (truck.checker)   details.push(`Checker: ${truck.checker}`);

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

    const driverOptions = settingsData.drivers.map(d =>
        `<option value="${d}" ${d === truck.driver ? 'selected' : ''}>${d}</option>`
    ).join('');

    const assistantOptions = settingsData.assistants.map(a =>
        `<option value="${a}" ${a === truck.assistant ? 'selected' : ''}>${a}</option>`
    ).join('');

    const checkerOptions = settingsData.checkers.map(c =>
        `<option value="${c}" ${c === truck.checker ? 'selected' : ''}>${c}</option>`
    ).join('');

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
    const newReg = document.getElementById(`edit-truck-reg-${index}`).value.trim().toUpperCase();

    if (!newReg) {
        alert('Please enter a registration number.');
        return;
    }

    const updatedTruck = {
        reg:       newReg,
        driver:    document.getElementById(`edit-truck-driver-${index}`).value    || '',
        assistant: document.getElementById(`edit-truck-assistant-${index}`).value || '',
        checker:   document.getElementById(`edit-truck-checker-${index}`).value   || ''
    };

    try {
        const response = await apiFetch(`${API_URL}/trucks/${encodeURIComponent(settingsData.trucks[index].reg)}`, {
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

async function addTruck() {
    const regInput       = document.getElementById('new-truck-reg');
    const driverSelect   = document.getElementById('new-truck-driver');
    const assistantSelect = document.getElementById('new-truck-assistant');
    const checkerSelect  = document.getElementById('new-truck-checker');

    const reg = regInput.value.trim().toUpperCase();
    if (!reg) {
        alert('Please enter a registration number.');
        return;
    }

    const newTruck = {
        reg,
        driver:    driverSelect.value    || '',
        assistant: assistantSelect.value || '',
        checker:   checkerSelect.value   || ''
    };

    try {
        const response = await apiFetch(`${API_URL}/trucks`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(newTruck)
        });

        if (response.ok) {
            settingsData.trucks.push(newTruck);
            regInput.value = '';
            driverSelect.value = '';
            assistantSelect.value = '';
            checkerSelect.value = '';
            renderTrucksList();
            lucide.createIcons();
        } else {
            alert('Could not save truck. Registration might already exist.');
        }
    } catch (e) {
        console.error(e);
        alert('Server error saving truck.');
    }
}

async function removeTruck(index) {
    const truck = settingsData.trucks[index];

    if (!confirm(`Are you sure you want to remove truck "${truck.reg}"?`)) return;

    try {
        const response = await apiFetch(`${API_URL}/trucks/${encodeURIComponent(truck.reg)}`, {
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

// ─────────────────────────────────────────────────────────
// CUSTOMER ROUTES
// ─────────────────────────────────────────────────────────

function renderCustomerRoutesList() {
    const listEl = document.getElementById('customer-routes-list');
    if (!listEl) return;

    const assignments = settingsData.customerRoutes || [];

    if (assignments.length === 0) {
        listEl.innerHTML = '<div class="settings-empty">No customer-route assignments yet</div>';
        return;
    }

    // Group by route
    const groupedByRoute = {};
    assignments.forEach(entry => {
        const route = entry.route_name;
        if (!groupedByRoute[route]) groupedByRoute[route] = [];
        groupedByRoute[route].push(entry);
    });

    let html = '';
    for (const [route, entries] of Object.entries(groupedByRoute)) {
        html += `<div class="settings-item-group">
            <div class="settings-item-group-header">
                <span class="route-badge">${route}</span>
                <span class="customer-count">${entries.length} customer${entries.length > 1 ? 's' : ''}</span>
            </div>`;

        entries.forEach(entry => {
            const customer = entry.customer_name;
            const mode     = entry.delivery_mode || 'INTERNAL';
            const escapedCustomer = customer.replace(/'/g, "\\'").replace(/"/g, "&quot;");
            const modeBadge = mode === 'THIRD_PARTY'
                ? `<span class="mode-badge mode-badge-third-party" title="Third Party delivery">3P</span>`
                : '';
            html += `
            <div class="settings-item" id="customer-route-item-${escapedCustomer.replace(/\s/g, '-')}">
                <div class="settings-item-info">
                    <span class="settings-item-name">${customer}</span>
                    ${modeBadge}
                </div>
                <div class="settings-item-actions">
                    <button class="btn-icon btn-edit" onclick="editCustomerRoute('${escapedCustomer}', '${route}', '${mode}')" title="Edit">
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

function editCustomerRoute(customerName, currentRoute, currentMode) {
    const escapedId = customerName.replace(/'/g, "\\'").replace(/"/g, "&quot;").replace(/\s/g, '-');
    const itemEl = document.getElementById(`customer-route-item-${escapedId}`);
    if (!itemEl) {
        // Fallback: find by text content
        const items = document.querySelectorAll('.settings-item');
        for (const item of items) {
            const nameSpan = item.querySelector('.settings-item-name');
            if (nameSpan && nameSpan.textContent === customerName) {
                showCustomerRouteEditForm(item, customerName, currentRoute, currentMode);
                return;
            }
        }
        return;
    }
    showCustomerRouteEditForm(itemEl, customerName, currentRoute, currentMode);
}

function showCustomerRouteEditForm(itemEl, customerName, currentRoute, currentMode) {
    const routeOptions = settingsData.routes.map(r =>
        `<option value="${r}" ${r === currentRoute ? 'selected' : ''}>${r}</option>`
    ).join('');

    itemEl.innerHTML = `
        <div class="settings-edit-form customer-route-edit">
            <input type="text" class="settings-edit-input" id="edit-customer-name" value="${customerName}">
            <select id="edit-customer-route">
                ${routeOptions}
            </select>
            <select id="edit-customer-delivery-mode">
                <option value="INTERNAL"    ${currentMode !== 'THIRD_PARTY' ? 'selected' : ''}>Internal</option>
                <option value="THIRD_PARTY" ${currentMode === 'THIRD_PARTY' ? 'selected' : ''}>Third Party</option>
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

    const input = document.getElementById('edit-customer-name');
    input.focus();
    input.select();

    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter')  saveCustomerRouteEdit(customerName);
        if (e.key === 'Escape') renderCustomerRoutesList();
    });

    lucide.createIcons();
}

async function saveCustomerRouteEdit(originalCustomerName) {
    const newCustomerName = document.getElementById('edit-customer-name').value.trim();
    const newRoute        = document.getElementById('edit-customer-route').value;
    const newMode         = document.getElementById('edit-customer-delivery-mode').value;

    if (!newCustomerName) { alert('Please enter a customer name.'); return; }
    if (!newRoute)         { alert('Please select a route.');        return; }

    try {
        // If the customer name changed, delete the old mapping first
        if (originalCustomerName !== newCustomerName) {
            await apiFetch(`${API_URL}/customer-routes/${encodeURIComponent(originalCustomerName)}`, { method: 'DELETE' });
        }
        const response = await apiFetch(`${API_URL}/customer-routes`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ customer_name: newCustomerName, route_name: newRoute, delivery_mode: newMode }),
        });
        if (!response.ok) { alert('Failed to save changes.'); return; }

        // Update local array
        settingsData.customerRoutes = settingsData.customerRoutes.filter(
            e => e.customer_name !== originalCustomerName
        );
        settingsData.customerRoutes.push({ customer_name: newCustomerName, route_name: newRoute, delivery_mode: newMode });
    } catch (e) {
        console.error(e);
        alert('Server error saving changes.');
        return;
    }

    renderCustomerRoutesList();
}

async function addCustomerRoute() {
    const customerInput  = document.getElementById('new-customer-name');
    const routeSelect    = document.getElementById('new-customer-route');
    const modeSelect     = document.getElementById('new-customer-delivery-mode');

    const customerName = customerInput.value.trim();
    const routeName    = routeSelect.value;
    const deliveryMode = modeSelect ? modeSelect.value : 'INTERNAL';

    if (!customerName) { alert('Please enter a customer name.'); return; }
    if (!routeName)    { alert('Please select a route.');        return; }

    // Check for duplicate (case-insensitive)
    const normalizedNew = customerName.toUpperCase();
    const existing = (settingsData.customerRoutes || [])
        .find(e => e.customer_name.toUpperCase() === normalizedNew);

    if (existing) {
        if (!confirm(`"${existing.customer_name}" is already assigned to "${existing.route_name}".\n\nUpdate this assignment?`)) return;
    }

    try {
        const response = await apiFetch(`${API_URL}/customer-routes`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ customer_name: customerName, route_name: routeName, delivery_mode: deliveryMode }),
        });

        if (response.ok) {
            // Remove old entry if updating, then push new
            settingsData.customerRoutes = (settingsData.customerRoutes || []).filter(
                e => e.customer_name.toUpperCase() !== normalizedNew
            );
            settingsData.customerRoutes.push({ customer_name: customerName, route_name: routeName, delivery_mode: deliveryMode });
            customerInput.value = '';
            routeSelect.value   = '';
            if (modeSelect) modeSelect.value = 'INTERNAL';
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

async function removeCustomerRoute(customerName) {
    if (!confirm(`Remove route assignment for "${customerName}"?`)) return;

    try {
        const response = await apiFetch(`${API_URL}/customer-routes/${encodeURIComponent(customerName)}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            settingsData.customerRoutes = (settingsData.customerRoutes || []).filter(
                e => e.customer_name !== customerName
            );
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

// ─────────────────────────────────────────────────────────
// PASSWORD VALIDATION
// ─────────────────────────────────────────────────────────

function validatePassword(password) {
    const errors = [];
    if (password.length < 10)        errors.push('At least 10 characters');
    if (!/[A-Z]/.test(password))     errors.push('1 uppercase letter');
    if (!/[a-z]/.test(password))     errors.push('1 lowercase letter');
    if (!/[0-9]/.test(password))     errors.push('1 digit');
    return errors;
}

function updatePasswordStrength(password, strengthEl, hintEl) {
    const errors = validatePassword(password);
    const bars   = strengthEl.querySelectorAll('.bar');
    const score  = 4 - errors.length;

    bars.forEach((bar, i) => {
        bar.className = 'bar';
        if (i < score) {
            bar.classList.add(score <= 1 ? 'weak' : score <= 2 ? 'medium' : 'strong');
        }
    });

    if (!password) {
        hintEl.textContent = '';
        hintEl.className = 'password-hint';
    } else if (errors.length > 0) {
        hintEl.textContent = 'Need: ' + errors.join(', ');
        hintEl.className = 'password-hint error';
    } else {
        hintEl.textContent = 'Strong password';
        hintEl.className = 'password-hint';
    }
    return errors;
}

// ─────────────────────────────────────────────────────────
// USER MANAGEMENT
// ─────────────────────────────────────────────────────────

async function renderUsersList() {
    const list = document.getElementById('users-list');
    list.innerHTML = '<div style="text-align:center; padding:1rem;">Loading users...</div>';

    try {
        const response = await apiFetch(`${API_URL}/users`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const data     = await response.json();
        const apiUsers = data.users || [];

        // Identify current user by stored username
        const currentUsername = localStorage.getItem('currentUser');

        list.innerHTML = '';
        apiUsers.forEach(user => {
            const div = document.createElement('div');
            div.className = 'user-card';

            const isSelf = currentUsername && currentUsername === user.username;

            const roleBadgeClass = user.role === ROLE_ADMIN        ? 'full-access'  :
                                   user.role === ROLE_DISPATCH     ? 'dispatch'     :
                                   user.role === ROLE_DRIVER       ? 'driver'       : 'reports-only';
            const roleBadgeText  = user.role === ROLE_ADMIN        ? 'Admin'        :
                                   user.role === ROLE_DISPATCH     ? 'Dispatch'     :
                                   user.role === ROLE_DRIVER       ? 'Delivery Operator' : 'Reports Only';

            const statusClass = user.is_active ? 'active'   : 'inactive';
            const statusText  = user.is_active ? 'Active'   : 'Inactive';

            div.innerHTML = `
                <div class="user-card-info">
                    <span class="user-card-name">${user.username} ${isSelf ? '<span class="self-badge">YOU</span>' : ''}</span>
                    <div class="user-card-meta">
                        <span class="role-badge ${roleBadgeClass}">${roleBadgeText}</span>
                        <span class="status-badge ${statusClass}">${statusText}</span>
                    </div>
                </div>
                <div class="user-card-actions">
                    ${!isSelf ? `
                        <select class="btn-sm" style="font-size:0.75rem; padding:0.3rem 0.4rem;" onchange="handleRoleChange(${user.id}, this.value, this)" title="Change role">
                            <option value="ADMIN"        ${user.role === 'ADMIN'        ? 'selected' : ''}>Admin</option>
                            <option value="DISPATCH"     ${user.role === 'DISPATCH'     ? 'selected' : ''}>Dispatch</option>
                            <option value="REPORTS_ONLY" ${user.role === 'REPORTS_ONLY' ? 'selected' : ''}>Reports Only</option>
                            <option value="DRIVER"       ${user.role === 'DRIVER'       ? 'selected' : ''}>Delivery Operator (Driver/Assistant)</option>
                        </select>
                        <label class="toggle-switch" title="${user.is_active ? 'Deactivate' : 'Activate'} user">
                            <input type="checkbox" ${user.is_active ? 'checked' : ''} onchange="handleStatusToggle(${user.id}, this.checked)">
                            <span class="toggle-slider"></span>
                        </label>
                    ` : '<span style="font-size:0.75rem; color:var(--text-light);">Cannot edit self</span>'}
                    <button class="btn-icon" onclick="showPasswordResetModal(${user.id}, '${user.username}')" title="Reset password">
                        <i data-lucide="key"></i>
                    </button>
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
    const nameInput  = document.getElementById('new-user-name');
    const pwdInput   = document.getElementById('new-user-password');
    const roleSelect = document.getElementById('new-user-role');
    const addBtn     = document.getElementById('add-user-btn');

    const username = nameInput.value.trim();
    const password = pwdInput.value.trim();
    const role     = roleSelect.value;

    if (!username || !password) {
        alert('Username and Password are required');
        return;
    }

    const pwErrors = validatePassword(password);
    if (pwErrors.length > 0) {
        alert('Password does not meet requirements:\n• ' + pwErrors.join('\n• '));
        return;
    }

    addBtn.disabled = true;
    addBtn.innerHTML = '<i data-lucide="loader-2" class="spin"></i> Adding...';
    lucide.createIcons();

    try {
        const response = await apiFetch(`${API_URL}/users`, {
            method: 'POST',
            body: JSON.stringify({ username, password, role, is_active: true })
        });

        if (!response.ok) {
            const err = await response.json();
            alert(err.detail || 'Failed to create user');
            return;
        }

        await renderUsersList();
        nameInput.value  = '';
        pwdInput.value   = '';
        roleSelect.value = ROLE_ADMIN;
        updatePasswordStrength(
            '',
            document.getElementById('new-user-pw-strength'),
            document.getElementById('new-user-pw-hint')
        );
    } catch (error) {
        console.error('Failed to add user:', error);
        alert('Failed to add user. Please check the server.');
    } finally {
        addBtn.disabled = false;
        addBtn.innerHTML = '<i data-lucide="user-plus"></i> Add User';
        lucide.createIcons();
    }
}

async function handleRoleChange(userId, newRole, selectEl) {
    try {
        const response = await apiFetch(`${API_URL}/users/${userId}/role`, {
            method: 'PUT',
            body: JSON.stringify({ role: newRole })
        });

        if (!response.ok) {
            const err = await response.json();
            alert(err.detail || 'Failed to update role');
            await renderUsersList();
            return;
        }
        await renderUsersList();
    } catch (error) {
        console.error('Failed to update role:', error);
        alert('Failed to update role.');
        await renderUsersList();
    }
}

async function handleStatusToggle(userId, isActive) {
    try {
        const response = await apiFetch(`${API_URL}/users/${userId}/status`, {
            method: 'PUT',
            body: JSON.stringify({ is_active: isActive })
        });

        if (!response.ok) {
            const err = await response.json();
            alert(err.detail || 'Failed to update status');
            await renderUsersList();
            return;
        }
        await renderUsersList();
    } catch (error) {
        console.error('Failed to update status:', error);
        alert('Failed to update status.');
        await renderUsersList();
    }
}

// ─────────────────────────────────────────────────────────
// PASSWORD RESET SUB-MODAL
// ─────────────────────────────────────────────────────────

function showPasswordResetModal(userId, username) {
    resetPasswordUserId = userId;
    document.getElementById('reset-pw-username').textContent = username;
    document.getElementById('reset-pw-input').value   = '';
    document.getElementById('reset-pw-confirm').value = '';
    updatePasswordStrength(
        '',
        document.getElementById('reset-pw-strength'),
        document.getElementById('reset-pw-hint')
    );
    document.getElementById('password-reset-modal').classList.add('visible');
    lucide.createIcons();
}

function hidePasswordResetModal() {
    resetPasswordUserId = null;
    document.getElementById('password-reset-modal').classList.remove('visible');
}

async function submitPasswordReset() {
    const password = document.getElementById('reset-pw-input').value;
    const confirm  = document.getElementById('reset-pw-confirm').value;
    const btn      = document.getElementById('reset-pw-submit-btn');

    if (!password)           { alert('Please enter a new password.'); return; }
    if (password !== confirm) { alert('Passwords do not match.');      return; }

    const pwErrors = validatePassword(password);
    if (pwErrors.length > 0) {
        alert('Password does not meet requirements:\n• ' + pwErrors.join('\n• '));
        return;
    }

    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="spin"></i> Resetting...';
    lucide.createIcons();

    try {
        const response = await apiFetch(`${API_URL}/users/${resetPasswordUserId}/password`, {
            method: 'PUT',
            body: JSON.stringify({ password })
        });

        if (!response.ok) {
            const err = await response.json();
            alert(err.detail || 'Failed to reset password');
            return;
        }

        alert('Password reset successfully.');
        hidePasswordResetModal();
    } catch (error) {
        console.error('Failed to reset password:', error);
        alert('Failed to reset password.');
    } finally {
        btn.disabled  = false;
        btn.innerHTML = '<i data-lucide="check"></i> Reset Password';
        lucide.createIcons();
    }
}

// ─────────────────────────────────────────────────────────
// WINDOW EXPORTS
// Functions called from dynamically generated onclick HTML
// must be accessible on the global window object.
// ─────────────────────────────────────────────────────────
window.editSettingsItem      = editSettingsItem;
window.saveSettingsEdit      = saveSettingsEdit;
window.cancelSettingsEdit    = cancelSettingsEdit;
window.removeSettingsItem    = removeSettingsItem;
window.editTruck             = editTruck;
window.saveTruckEdit         = saveTruckEdit;
window.cancelTruckEdit       = cancelTruckEdit;
window.removeTruck           = removeTruck;
window.editCustomerRoute     = editCustomerRoute;
window.saveCustomerRouteEdit = saveCustomerRouteEdit;
window.renderCustomerRoutesList = renderCustomerRoutesList;
window.removeCustomerRoute   = removeCustomerRoute;
window.handleRoleChange      = handleRoleChange;
window.handleStatusToggle    = handleStatusToggle;
window.showPasswordResetModal  = showPasswordResetModal;
window.hidePasswordResetModal  = hidePasswordResetModal;
window.submitPasswordReset     = submitPasswordReset;

// ─────────────────────────────────────────────────────────
// PAGE BOOTSTRAP
// ─────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
    // Auth check — redirect to index.html if not logged in
    requireAuth();

    // Role check — settings page is ADMIN only
    const role = getUserRole();
    if (role !== ROLE_ADMIN) {
        window.location.href = 'index.html';
        return;
    }

    // Load settings data from server
    await loadSettings();

    // Render all tab lists
    renderSettingsList('drivers');
    renderSettingsList('assistants');
    renderSettingsList('checkers');
    renderSettingsList('routes');
    renderTrucksList();
    renderCustomerRoutesList();

    // Populate dropdowns
    populateTruckFormDropdowns();
    populateCustomerRouteDropdown();
    populateCustomerSuggestions();

    // Tab switching
    document.querySelectorAll('.settings-tab').forEach(tab => {
        tab.addEventListener('click', handleSettingsTabClick);
    });

    // Users tab: load user list on first click
    const usersTab = document.querySelector('.settings-tab[data-tab="users"]');
    if (usersTab) {
        usersTab.addEventListener('click', renderUsersList);
    }

    // Add buttons
    document.getElementById('add-driver-btn').addEventListener('click',    () => addSettingsItem('drivers'));
    document.getElementById('add-assistant-btn').addEventListener('click', () => addSettingsItem('assistants'));
    document.getElementById('add-checker-btn').addEventListener('click',   () => addSettingsItem('checkers'));
    document.getElementById('add-route-btn').addEventListener('click',     () => addSettingsItem('routes'));
    document.getElementById('add-truck-btn').addEventListener('click',     addTruck);
    document.getElementById('add-customer-route-btn').addEventListener('click', addCustomerRoute);
    document.getElementById('add-user-btn').addEventListener('click',      addUser);

    // Enter key on text inputs
    ['new-driver-name', 'new-assistant-name', 'new-checker-name', 'new-route-name'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('keyup', e => {
            if (e.key === 'Enter') {
                const category = id.replace('new-', '').replace('-name', '') + 's';
                addSettingsItem(category);
            }
        });
    });

    const truckRegInput = document.getElementById('new-truck-reg');
    if (truckRegInput) truckRegInput.addEventListener('keyup', e => { if (e.key === 'Enter') addTruck(); });

    // Password strength indicators
    const newPwInput = document.getElementById('new-user-password');
    if (newPwInput) {
        newPwInput.addEventListener('input', () => updatePasswordStrength(
            newPwInput.value,
            document.getElementById('new-user-pw-strength'),
            document.getElementById('new-user-pw-hint')
        ));
    }

    const resetPwInput = document.getElementById('reset-pw-input');
    if (resetPwInput) {
        resetPwInput.addEventListener('input', () => updatePasswordStrength(
            resetPwInput.value,
            document.getElementById('reset-pw-strength'),
            document.getElementById('reset-pw-hint')
        ));
    }

    lucide.createIcons();
});

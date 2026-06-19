// =============================================
// MANUAL ENTRY & RESTORE FUNCTIONS
// =============================================
// Depends on: apiFetch (auth.js), API_URL (script.js)
// Loaded after auth.js and script.js.
// Initialized via initManualEntryRestoreListeners(), called from initSecondaryListeners() in script.js.

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
    const currencyEl = document.getElementById('manual-currency');
    const currency = currencyEl ? currencyEl.value : 'USD';
    const area = document.getElementById('manual-area').value.trim();

    if (!invoiceNum || !orderNum || !customer || !value) {
        alert("Please fill in all required fields marked with *");
        return;
    }

    try {
        const response = await apiFetch(`${API_URL}/invoices/manual`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                invoice_number: invoiceNum,
                order_number: orderNum,
                customer_name: customer,
                customer_number: customerNumber,
                total_value: value,
                currency: currency,
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
            if (currencyEl) currencyEl.value = 'USD';
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
            const response = await apiFetch(`${API_URL}/invoices/search?q=${encodeURIComponent(query)}`);
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
        const response = await apiFetch(`${API_URL}/invoices/restore`, {
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

function initManualEntryRestoreListeners() {
    document.getElementById('close-manual-modal-btn').addEventListener('click', closeManualEntryModal);
    document.getElementById('tab-manual-entry-btn').addEventListener('click', () => switchManualModalTab('manual'));
    document.getElementById('tab-restore-history-btn').addEventListener('click', () => switchManualModalTab('restore'));
    document.getElementById('submit-manual-entry-btn').addEventListener('click', submitManualEntry);
    document.getElementById('restore-search-input').addEventListener('input', searchRestoreHistory);
    document.getElementById('restore-select-all').addEventListener('change', toggleSelectAllRestore);
    document.getElementById('restore-btn').addEventListener('click', restoreSelectedInvoices);
}


// =============================================
// INITIALIZATION
// =============================================

function hideReportPrintPreview() {
    const modal = document.getElementById('report-preview-modal');
    modal.classList.add('hidden');
    modal.classList.remove('visible');
}

document.addEventListener('DOMContentLoaded', () => {
    // Basic setup
    setupEventListeners();
    setDefaultDate();
    initManifestNumber();

    console.log('Validating connection...');

    // Explicitly show landing page
    showLandingPage();

    // Attempt background load
    loadSettings();
});

// =============================================
// MIGRATION FUNCTION
// =============================================

async function migrateLocalData() {
    const btn = document.getElementById('migrate-btn');
    const status = document.getElementById('migration-status');

    if (!confirm("This will upload your local Settings and Reports history to the server. Continue?")) {
        return;
    }

    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2"></i> Syncing...';
    status.textContent = "Starting migration...";
    status.style.color = "#3b82f6";

    try {
        // 1. Migrate Settings
        const savedSettings = localStorage.getItem('manifestSettings');
        if (savedSettings) {
            const data = JSON.parse(savedSettings);

            // Drivers
            for (const item of data.drivers || []) {
                await apiFetch(`${API_URL}/settings`, {
                    method: 'POST',
                    body: JSON.stringify({ category: 'drivers', value: item })
                });
            }

            // Assistants
            for (const item of data.assistants || []) {
                await apiFetch(`${API_URL}/settings`, {
                    method: 'POST',
                    body: JSON.stringify({ category: 'assistants', value: item })
                });
            }

            // Checkers
            for (const item of data.checkers || []) {
                await apiFetch(`${API_URL}/settings`, {
                    method: 'POST',
                    body: JSON.stringify({ category: 'checkers', value: item })
                });
            }

            // Routes
            for (const item of data.routes || []) {
                await apiFetch(`${API_URL}/settings`, {
                    method: 'POST',
                    body: JSON.stringify({ category: 'routes', value: item })
                });
            }

            // Trucks
            for (const truck of data.trucks || []) {
                await apiFetch(`${API_URL}/trucks`, {
                    method: 'POST',
                    body: JSON.stringify(truck)
                });
            }

            // Customer Routes
            for (const [customer, route] of Object.entries(data.customerRoutes || {})) {
                await apiFetch(`${API_URL}/customer-routes`, {
                    method: 'POST',
                    body: JSON.stringify({ customer_name: customer, route_name: route })
                });
            }
        }

        // 2. Migrate Reports
        const savedReports = localStorage.getItem('manifestReports');
        if (savedReports) {
            const reports = JSON.parse(savedReports);
            for (const report of reports) {
                await apiFetch(`${API_URL}/reports`, {
                    method: 'POST',
                    body: JSON.stringify(report)
                });
            }
        }

        status.textContent = "Sync Complete! Data is now on the server.";
        status.style.color = "#10b981";

        await loadSettings();
        alert("Sync Complete! You can now access this data from other computers.");

    } catch (e) {
        console.error(e);
        status.textContent = "Error during sync. Check console.";
        status.style.color = "#ef4444";
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="upload-cloud"></i> Sync Data to Server';
        lucide.createIcons();
    }
}

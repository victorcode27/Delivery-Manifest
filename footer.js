
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


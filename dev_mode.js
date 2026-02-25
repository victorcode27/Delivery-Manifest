/**
 * DevModeSystem
 * Handles toggleable DEV_MODE features including Badge, Debug Panel, and Inline Warnings.
 */

class DevModeSystem {
    constructor() {
        this.isEnabled = false;
        this.initialServerTime = null;
        this.currentServerTime = null;
        this.frontendLoadTime = new Date().toISOString();
        this.pollInterval = null;
        this.logs = [];

        // DOM Elements
        this.badge = null;
        this.panel = null;
    }

    async init() {
        console.log("DevModeSystem initializing...");
        try {
            const health = await this.fetchHealth();
            if (health && health.dev_mode) {
                this.isEnabled = true;
                this.initialServerTime = health.timestamp;
                this.currentServerTime = health.timestamp;

                this.renderBadge();
                this.renderPanel();
                this.attachInputWarnings();
                this.startPolling();

                this.log("DevMode initialized. DEV_MODE = true");
            } else {
                console.log("DevMode disabled by server.");
            }
        } catch (e) {
            console.error("Failed to initialize DevMode:", e);
        }
    }

    async fetchHealth() {
        try {
            const res = await fetch(`/health`);
            if (res.ok) {
                return await res.json();
            }
        } catch (e) {
            console.warn("DevMode health check failed:", e);
            return null;
        }
    }

    renderBadge() {
        // Remove existing badge if present
        const existing = document.getElementById('dev-mode-badge');
        if (existing) existing.remove();

        this.badge = document.createElement('div');
        this.badge.id = 'dev-mode-badge';
        this.badge.className = 'status-ok'; // Default green
        this.badge.innerHTML = `
            <span>DEV MODE</span>
            <span id="dev-status-indicator">●</span>
        `;

        this.badge.addEventListener('click', () => this.togglePanel());
        document.body.appendChild(this.badge);
    }

    renderPanel() {
        const existing = document.getElementById('dev-debug-panel');
        if (existing) existing.remove();

        this.panel = document.createElement('div');
        this.panel.id = 'dev-debug-panel';
        this.panel.innerHTML = `
            <div class="debug-header">
                <span>Debug Panel</span>
                <span style="font-size: 10px; opacity: 0.7;">v1.0</span>
            </div>
            <div class="debug-content">
                <div class="debug-row">
                    <span class="debug-label">Frontend Load:</span>
                    <span class="debug-value">${new Date(this.frontendLoadTime).toLocaleTimeString()}</span>
                </div>
                <div class="debug-row">
                    <span class="debug-label">Server Start:</span>
                    <span class="debug-value" id="dev-server-time">${new Date(this.initialServerTime).toLocaleTimeString()}</span>
                </div>
                <div class="debug-row">
                    <span class="debug-label">Status:</span>
                    <span class="debug-value" id="dev-connection-status">Connected</span>
                </div>
                <div class="debug-log" id="dev-log-container">
                    <!-- Logs go here -->
                </div>
            </div>
        `;
        document.body.appendChild(this.panel);
    }

    togglePanel() {
        if (this.panel) {
            this.panel.classList.toggle('visible');
        }
    }

    startPolling() {
        // Poll every 5 seconds
        this.pollInterval = setInterval(async () => {
            const health = await this.fetchHealth();
            this.updateStatus(health);
        }, 5000);
    }

    updateStatus(health) {
        const statusEl = document.getElementById('dev-connection-status');
        const badge = document.getElementById('dev-mode-badge');

        if (!health) {
            // Unreachable
            if (badge) badge.className = 'status-error';
            if (statusEl) {
                statusEl.textContent = "Unreachable";
                statusEl.style.color = "#ef4444";
            }
            return;
        }

        this.currentServerTime = health.timestamp;

        // Logic: 
        // If currentServerTime != initialServerTime -> Blue (Restarted)
        // Else -> Green (OK)

        if (this.currentServerTime !== this.initialServerTime) {
            if (badge) badge.className = 'status-restarted';
            if (statusEl) {
                statusEl.textContent = "Server Restarted";
                statusEl.style.color = "#3b82f6";
            }
        } else {
            if (badge) badge.className = 'status-ok';
            if (statusEl) {
                statusEl.textContent = "Connected";
                statusEl.style.color = "#22c55e";
            }
        }
    }

    attachInputWarnings() {
        // Find all date inputs
        const dateInputs = document.querySelectorAll('input[type="date"]');
        dateInputs.forEach(input => {
            // Create warning element container
            const warningId = `warning-${input.id}`;
            let warningEl = document.getElementById(warningId);

            input.addEventListener('input', (e) => {
                this.validateDateInput(e.target);
            });

            // Also validate on blur
            input.addEventListener('blur', (e) => {
                this.validateDateInput(e.target);
            });
        });
    }

    validateDateInput(input) {
        const val = input.value;
        const parent = input.parentElement;

        // Remove existing warning
        let warning = parent.querySelector('.dev-warning-message');
        if (warning) warning.remove();

        if (!val) {
            // Empty is usually valid for filters, but maybe warn if it's a required field context? 
            // User prompt: "Examples: ⚠ Empty date filter ignored"
            this.showWarning(parent, "⚠ Empty date filter ignored");
            return;
        }

        // Check Validity (Native browser check)
        if (!input.checkValidity()) {
            this.showWarning(parent, "❌ Invalid date format");
        }
    }

    showWarning(parent, message) {
        const div = document.createElement('div');
        div.className = 'dev-warning-message';
        div.textContent = message;
        parent.appendChild(div);
    }

    log(msg) {
        const container = document.getElementById('dev-log-container');
        if (container) {
            const entry = document.createElement('div');
            entry.className = 'log-entry';
            entry.innerHTML = `<span style="opacity:0.6">[${new Date().toLocaleTimeString()}]</span> ${msg}`;
            container.appendChild(entry);
            container.scrollTop = container.scrollHeight;
        }
        this.logs.push(msg);
    }
}

// Instantiate globally
window.DevMode = new DevModeSystem();

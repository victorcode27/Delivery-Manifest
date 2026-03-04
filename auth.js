/**
 * auth.js — Shared authentication utilities
 * Loaded before page scripts in all HTML pages.
 *
 * Provides:
 *   getToken()           — read JWT from localStorage
 *   setToken(token)      — persist JWT to localStorage
 *   setCurrentUser(name) — persist username to localStorage (X-Username fallback)
 *   clearToken()         — remove JWT + username from localStorage
 *   requireAuth()        — redirect to index.html if not authenticated
 *   apiFetch(url, opts)  — fetch wrapper that attaches auth headers and handles 401/403
 */

function getToken() {
    return localStorage.getItem('accessToken');
}

function setToken(token) {
    if (token) {
        localStorage.setItem('accessToken', token);
    }
}

function setCurrentUser(username) {
    if (username) {
        localStorage.setItem('currentUser', username);
    }
}

function clearToken() {
    localStorage.removeItem('accessToken');
    localStorage.removeItem('currentUser');
}

/**
 * Redirect to index.html if neither a JWT nor a stored username exists.
 * Supports both production (JWT) and local dev (X-Username) modes.
 */
function requireAuth() {
    const token = getToken();
    const user = localStorage.getItem('currentUser');
    if (!token && !user) {
        window.location.href = 'index.html';
    }
}

/**
 * Authenticated fetch wrapper.
 * - Attaches Authorization: Bearer <token> when JWT is present.
 * - Falls back to X-Username header when only a stored username exists (local dev).
 * - Strips Content-Type for FormData so the browser sets the multipart boundary.
 * - On 401: alerts the user, clears the session, and redirects to index.html.
 * - On 403: alerts the user and returns the response (no redirect).
 */
async function apiFetch(url, options = {}) {
    const token = getToken();
    const storedUser = localStorage.getItem('currentUser');
    const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };

    // FormData: let the browser set Content-Type (includes multipart boundary)
    if (options.body instanceof FormData) {
        delete headers['Content-Type'];
    }

    if (token) {
        headers['Authorization'] = `Bearer ${token}`;
    } else if (storedUser) {
        headers['X-Username'] = storedUser;
    }

    const response = await fetch(url, { ...options, headers });

    if (response.status === 401) {
        alert('Session expired. Please log in again.');
        clearToken();
        window.location.href = 'index.html';
        return response;
    }

    if (response.status === 403) {
        alert('Access denied.');
        return response;
    }

    return response;
}

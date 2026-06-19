// =============================================
// CURRENCY FORMATTING HELPERS
// =============================================
// Shared by script.js, analytics.js, manual_entry.js.
// Load this file before all three.

const SUPPORTED_CURRENCIES = ['USD', 'ZWL'];
const DEFAULT_CURRENCY = 'USD';

/**
 * Format a single monetary value with its currency code.
 * Missing/blank currency defaults to USD for backward compatibility.
 *
 *   formatCurrencyValue(1234.5, 'ZWL') -> "ZWL 1,234.50"
 *   formatCurrencyValue(1234.5)        -> "USD 1,234.50"
 */
function formatCurrencyValue(value, currency) {
    const cur = (currency && String(currency).trim())
        ? String(currency).trim().toUpperCase()
        : DEFAULT_CURRENCY;
    // total_value from the API can be a string with thousands-separator
    // commas (the parser preserves the PDF's printed format verbatim, e.g.
    // "1,897.99") — Number() on a comma-containing string is NaN, which
    // silently rendered as "0.00". Strip non-numeric formatting first.
    const num = Number(String(value).replace(/[^0-9.-]/g, ''));
    const formatted = (isNaN(num) ? 0 : num).toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
    return `${cur} ${formatted}`;
}

/**
 * Render a totals_by_currency array (Stage 4 analytics response shape) as
 * one line per currency, joined for innerHTML display — never blends
 * USD and ZWL into a single combined figure.
 *
 *   formatTotalsByCurrency([{currency:'USD', total_value:450}, {currency:'ZWL', total_value:20000}])
 *   -> "USD 450.00<br>ZWL 20,000.00"
 *
 * Returns '—' when the list is empty or missing.
 */
function formatTotalsByCurrency(totalsByCurrency, valueKey = 'total_value') {
    if (!Array.isArray(totalsByCurrency) || totalsByCurrency.length === 0) {
        return '—';
    }
    return totalsByCurrency
        .map(t => formatCurrencyValue(t[valueKey], t.currency))
        .join('<br>');
}

/**
 * Sum a count-style field (invoice_count, manifest_count, etc.) across a
 * totals_by_currency array. Safe to use for counts — never use this to sum
 * monetary fields, which must stay separated by currency.
 */
function sumAcrossCurrencies(totalsByCurrency, key) {
    if (!Array.isArray(totalsByCurrency)) return 0;
    return totalsByCurrency.reduce((sum, t) => sum + (Number(t[key]) || 0), 0);
}

/**
 * Group a list of order-like objects ({value, currency}) into per-currency
 * totals. Missing currency defaults to USD. Used by the manifest builder to
 * show separate USD/ZWL totals instead of one blended sum.
 *
 *   calcValueTotalsByCurrency([{value:100,currency:'USD'},{value:50,currency:'ZWL'}])
 *   -> [{currency:'USD', value:100}, {currency:'ZWL', value:50}]
 */
function calcValueTotalsByCurrency(items, valueField = 'value', currencyField = 'currency') {
    const byCurrency = {};
    (items || []).forEach(item => {
        const cur = (item[currencyField] && String(item[currencyField]).trim())
            ? String(item[currencyField]).trim().toUpperCase()
            : DEFAULT_CURRENCY;
        const itemNum = Number(String(item[valueField]).replace(/[^0-9.-]/g, ''));
        byCurrency[cur] = (byCurrency[cur] || 0) + (isNaN(itemNum) ? 0 : itemNum);
    });
    return Object.keys(byCurrency).sort().map(currency => ({ currency, value: byCurrency[currency] }));
}

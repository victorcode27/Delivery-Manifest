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

// =============================================
// VAT BREAKDOWN HELPERS
// =============================================

// Current VAT rate (15.5%). Single source of truth — never hardcode 0.155
// or 1.155 elsewhere; import/reference this constant instead.
const VAT_RATE = 0.155;

/**
 * Compute the VAT breakdown of a VAT-inclusive total.
 *
 *   total_excluding_vat = total_including_vat / (1 + vatRate)
 *   vat_amount           = total_including_vat - total_excluding_vat
 *
 * Accepts a number or a comma-formatted string (e.g. "1,155.00"). Blank,
 * null, undefined, or otherwise non-numeric input is treated as 0 — this
 * function never returns NaN/undefined/null.
 *
 * Values are returned unrounded (full float precision) so callers can sum
 * many breakdowns before rounding once at display time, instead of
 * compounding rounding error across rows.
 *
 *   calculateVatBreakdown(1155)        -> { totalInclVat: 1155, totalExclVat: 1000, vatAmount: 155 }
 *   calculateVatBreakdown("1,155.00")  -> same result
 *   calculateVatBreakdown(null)        -> { totalInclVat: 0, totalExclVat: 0, vatAmount: 0 }
 */
function calculateVatBreakdown(totalInclVat, vatRate = VAT_RATE) {
    const parsed = Number(String(totalInclVat ?? '').replace(/[^0-9.-]/g, ''));
    const incl = isNaN(parsed) ? 0 : parsed;
    const excl = incl / (1 + vatRate);
    const vat  = incl - excl;
    return {
        totalInclVat: incl,
        totalExclVat: excl,
        vatAmount:    vat,
    };
}

/**
 * Render a per-currency VAT breakdown as display-ready text — one block of
 * three lines (Excl VAT / VAT / Incl VAT) per currency, joined with <br>.
 * Each currency's breakdown is computed independently; USD and ZWL amounts
 * are never combined.
 *
 * Input follows the same {currency, value} shape produced by
 * calcValueTotalsByCurrency():
 *
 *   formatVatBreakdownByCurrency([{currency:'USD', value:1155}, {currency:'ZWL', value:2310}])
 *   -> "USD Excl VAT: 1,000.00<br>USD VAT: 155.00<br>USD Incl VAT: 1,155.00<br>
 *       ZWL Excl VAT: 2,000.00<br>ZWL VAT: 310.00<br>ZWL Incl VAT: 2,310.00"
 *
 * Returns '—' when the list is empty or missing.
 */
function formatVatBreakdownByCurrency(totalsByCurrency, valueKey = 'value', vatRate = VAT_RATE) {
    if (!Array.isArray(totalsByCurrency) || totalsByCurrency.length === 0) {
        return '—';
    }
    // formatCurrencyValue() always returns "<CUR> <number>" — reuse it for the
    // numeric formatting, then drop its own currency prefix since we apply
    // our own "<CUR> <label>:" prefix per line below.
    const formatAmount = (value, currency) => {
        const formatted = formatCurrencyValue(value, currency);
        return formatted.slice(formatted.indexOf(' ') + 1);
    };
    return totalsByCurrency
        .map(t => {
            const cur = (t.currency && String(t.currency).trim())
                ? String(t.currency).trim().toUpperCase()
                : DEFAULT_CURRENCY;
            const { totalExclVat, vatAmount, totalInclVat } = calculateVatBreakdown(t[valueKey], vatRate);
            return [
                `${cur} Excl VAT: ${formatAmount(totalExclVat, cur)}`,
                `${cur} VAT: ${formatAmount(vatAmount, cur)}`,
                `${cur} Incl VAT: ${formatAmount(totalInclVat, cur)}`,
            ].join('<br>');
        })
        .join('<br>');
}

/**
 * Format an epoch-seconds timestamp as a relative-age string
 * (e.g. "just now", "12m ago", "3h ago", "2d ago").
 *
 * Shared between commands that surface cache/report freshness — keep the
 * thresholds in one place so the same fetch_at renders identically across
 * `analyze`, `report`, etc.
 */
export function formatAge(epochSeconds: number): string {
  const ageMs = Date.now() - epochSeconds * 1000;
  const mins = Math.floor(ageMs / 60_000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

/**
 * Parse an Octagon API timestamp, reading one without a zone suffix as UTC.
 *
 * Not every timestamp field is typed date-time in the API schema
 * (`analysis_last_updated` is a plain string), and `new Date()` reads a
 * zone-less value as LOCAL time. Mirrors octagon-web's parseServerUtcTimestamp.
 * Returns null for a missing or unparseable value.
 */
export function parseUtcTimestamp(value: string | null | undefined): Date | null {
  let v = (value ?? '').trim();
  if (!v) return null;
  v = v.replace(/\s+([zZ]|[+-]\d{2}(?::?\d{2})?)$/, '$1'); // "… +00:00" → "…+00:00"
  if (v.includes(' ') && !v.includes('T')) v = v.replace(/\s+/, 'T');
  // A zone suffix only counts after a time part, so a date-only "2026-09-02"
  // isn't read as carrying a "-02" offset.
  const time = v.includes('T') ? v.slice(v.indexOf('T')) : '';
  if (/[zZ]$/.test(time) || /[+-]\d{2}:\d{2}$/.test(time)) {
    // already explicit
  } else if (/[+-]\d{4}$/.test(time)) {
    v = `${v.slice(0, -2)}:${v.slice(-2)}`;
  } else if (/[+-]\d{2}$/.test(time)) {
    v = `${v}:00`;
  } else if (time) {
    v = `${v}Z`;
  }
  const date = new Date(v);
  return Number.isNaN(date.getTime()) ? null : date;
}

/** How far apart the two stamps may drift before they are named separately. */
const ANALYSIS_PROVENANCE_THRESHOLD_MS = 24 * 60 * 60 * 1000;

/**
 * True when a snapshot's analysis is meaningfully older than its capture.
 *
 * A scheduled refresh carries a prior run's analysis forward over fresh market
 * data: `captured_at` moves while `analysis_last_updated` stays put, so the
 * model numbers are older than the market numbers. Below a day of drift the
 * distinction is noise; missing or unparseable stamps make no claim. Mirrors
 * octagon-web's analysisPredatesCapture (src/lib/domain/report-freshness.ts).
 */
export function analysisPredatesCapture(
  capturedAt: string | null | undefined,
  analysisLastUpdated: string | null | undefined,
): boolean {
  const captured = parseUtcTimestamp(capturedAt)?.getTime();
  const analysis = parseUtcTimestamp(analysisLastUpdated)?.getTime();
  if (captured === undefined || analysis === undefined) return false;
  return captured - analysis > ANALYSIS_PROVENANCE_THRESHOLD_MS;
}

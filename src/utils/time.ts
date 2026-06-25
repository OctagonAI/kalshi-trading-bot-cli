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

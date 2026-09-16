import { describe, test, expect } from 'bun:test';
import { shouldRunIncremental } from '../search-index.js';

const HOUR = 60 * 60 * 1000;
const now = Date.UTC(2026, 8, 15, 20, 0, 0);

describe('shouldRunIncremental', () => {
  test('a forced refresh is never incremental', () => {
    // The regression: forceRefreshIndex() met every other condition — populated
    // index, refreshed minutes ago — so it fetched a min_updated_ts delta and
    // skipped its staleness sweep. Live, that produced 4,882 rows where a real
    // rebuild produces ~12,700.
    expect(shouldRunIncremental(true, now - HOUR, 13_811, now)).toBe(false);
  });

  test('a recent, populated index is incremental', () => {
    expect(shouldRunIncremental(false, now - HOUR, 13_811, now)).toBe(true);
  });

  test('an index older than the TTL rebuilds in full', () => {
    expect(shouldRunIncremental(false, now - 25 * HOUR, 13_811, now)).toBe(false);
  });

  test('an empty index rebuilds in full however fresh the stamp', () => {
    // A 0-row index can carry a recent last_refresh; trusting the timestamp
    // alone would leave it empty behind an incremental delta.
    expect(shouldRunIncremental(false, now - HOUR, 0, now)).toBe(false);
  });

  test('a never-refreshed index rebuilds in full', () => {
    expect(shouldRunIncremental(false, null, 0, now)).toBe(false);
    expect(shouldRunIncremental(false, null, 13_811, now)).toBe(false);
  });

  test('the TTL boundary is exclusive', () => {
    expect(shouldRunIncremental(false, now - 24 * HOUR, 13_811, now)).toBe(false);
    expect(shouldRunIncremental(false, now - 24 * HOUR + 1, 13_811, now)).toBe(true);
  });
});

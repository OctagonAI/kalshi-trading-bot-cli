import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import type { Database } from 'bun:sqlite';
import { createDb } from '../index.js';
import { clearAndPopulateIndex, countIndexRows, getIndexAge, setLastRefresh } from '../event-index.js';

function event(ticker: string) {
  return { event_ticker: ticker, title: `Title ${ticker}`, series_ticker: 'KXTEST' };
}

describe('event index population', () => {
  let db: Database;
  beforeEach(() => { db = createDb(':memory:'); });
  afterEach(() => { db.close(); });

  test('populates and counts rows', () => {
    clearAndPopulateIndex(db, [event('KXA'), event('KXB')]);
    expect(countIndexRows(db)).toBe(2);
  });

  test('refuses an empty result set instead of wiping a good index', () => {
    clearAndPopulateIndex(db, [event('KXA'), event('KXB')]);
    expect(() => clearAndPopulateIndex(db, [])).toThrow(/empty result set/i);
    // The existing rows must survive the refusal.
    expect(countIndexRows(db)).toBe(2);
  });

  test('countIndexRows is 0 on a fresh db even when a refresh was stamped', () => {
    // The failure mode this guards: a 0-row index that still looks fresh.
    setLastRefresh(db, Date.now());
    expect(getIndexAge(db)).toBeLessThan(1000);
    expect(countIndexRows(db)).toBe(0);
  });
});

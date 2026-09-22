import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import type { Database } from 'bun:sqlite';
import { createDb } from '../index.js';
import type { KalshiMarket } from '../../tools/kalshi/types.js';
import { clearAndPopulateIndex, countIndexRows, getIndexAge, searchEventIndex, setLastRefresh } from '../event-index.js';

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

function market(ticker: string, volume: number, result = ''): KalshiMarket {
  return { ticker, status: 'active', close_time: '2099-01-01T00:00:00Z', result, volume } as unknown as KalshiMarket;
}

describe('searchEventIndex tradeable predicate', () => {
  let db: Database;
  beforeEach(() => { db = createDb(':memory:'); });
  afterEach(() => { db.close(); });

  test('an event whose only market has settled is not returned', () => {
    // status alone does not mean tradeable: a market can settle upstream while
    // the index still carries status 'active'.
    clearAndPopulateIndex(db, [
      { event_ticker: 'SETTLED', title: 'crypto settled', markets: [market('SETTLED-M', 100, 'yes')] },
      { event_ticker: 'LIVE', title: 'crypto live', markets: [market('LIVE-M', 100)] },
    ]);
    expect(searchEventIndex(db, 'crypto').map((e) => e.event_ticker)).toEqual(['LIVE']);
  });

  test('a settled market contributes no volume to the ranking', () => {
    // The filter and the ranking have to agree, or a settled market keeps
    // pushing its event up the list after it stops being tradeable.
    clearAndPopulateIndex(db, [
      { event_ticker: 'MIXED', title: 'crypto mixed', markets: [market('MIXED-M', 10), market('MIXED-T2', 999_999, 'yes')] },
      { event_ticker: 'BUSY', title: 'crypto busy', markets: [market('BUSY-M', 500)] },
    ]);
    expect(searchEventIndex(db, 'crypto').map((e) => e.event_ticker)).toEqual(['BUSY', 'MIXED']);
  });
});

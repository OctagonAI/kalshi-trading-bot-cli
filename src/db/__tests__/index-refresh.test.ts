import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import type { Database } from 'bun:sqlite';
import type { KalshiMarket } from '../../tools/kalshi/types.js';
import { createDb } from '../index.js';
import { countIndexRows, pruneStaleEvents, upsertIndexEvents } from '../event-index.js';

function market(over: Partial<KalshiMarket>): KalshiMarket {
  return {
    ticker: 'KXA-T1',
    title: 'Title',
    yes_sub_title: '$1 or above',
    status: 'active',
    close_time: '2099-01-01T00:00:00Z',
    result: '',
    ...over,
  } as unknown as KalshiMarket;
}

function event(ticker: string, markets: KalshiMarket[] = [market({ ticker: `${ticker}-T1` })]) {
  return { event_ticker: ticker, title: `Title ${ticker}`, series_ticker: 'KXTEST', markets };
}

describe('upsertIndexEvents', () => {
  let db: Database;
  beforeEach(() => { db = createDb(':memory:'); });
  afterEach(() => { db.close(); });

  test('a streamed build accumulates pages instead of replacing them', () => {
    // The refresh writes page by page now, so the index has to grow across
    // calls — the old clear-then-insert would have left only the last page.
    upsertIndexEvents(db, [event('KXA'), event('KXB')]);
    upsertIndexEvents(db, [event('KXC')]);
    expect(countIndexRows(db)).toBe(3);
  });

  test('re-upserting an event updates it rather than duplicating it', () => {
    upsertIndexEvents(db, [event('KXA')]);
    upsertIndexEvents(db, [{ ...event('KXA'), title: 'Renamed' }]);
    expect(countIndexRows(db)).toBe(1);
    const row = db.query('SELECT title FROM event_index WHERE event_ticker = ?').get('KXA') as { title: string };
    expect(row.title).toBe('Renamed');
  });

  test('tags survive an upsert', () => {
    // Tags arrive from a separate per-series pass after the events land, so an
    // upsert that wrote them would blank them on every refresh.
    upsertIndexEvents(db, [event('KXA')]);
    db.query('UPDATE event_index SET tags = ? WHERE event_ticker = ?').run('Bitcoin,Crypto', 'KXA');
    upsertIndexEvents(db, [{ ...event('KXA'), title: 'Renamed' }]);
    const row = db.query('SELECT tags FROM event_index WHERE event_ticker = ?').get('KXA') as { tags: string | null };
    expect(row.tags).toBe('Bitcoin,Crypto');
  });
});

describe('pruneStaleEvents', () => {
  let db: Database;
  beforeEach(() => { db = createDb(':memory:'); });
  afterEach(() => { db.close(); });

  test('a full build evicts events the build never saw — the delisted case', () => {
    // KXBTCD-33APR0610 is the real example: Kalshi 404s it, but it still looked
    // "active" in the index because its markets carry a 2033 close date. Only
    // "this build did not see it" catches that.
    upsertIndexEvents(db, [event('KXGONE'), event('KXLIVE')]);
    const buildStart = Date.now() + 1000; // every existing row predates this build
    upsertIndexEvents(db, [event('KXLIVE')]); // only this one is re-seen
    db.query('UPDATE event_index SET indexed_at = ? WHERE event_ticker = ?').run(buildStart, 'KXLIVE');

    expect(pruneStaleEvents(db, buildStart)).toBe(1);
    expect(countIndexRows(db)).toBe(1);
    const left = db.query('SELECT event_ticker FROM event_index').get() as { event_ticker: string };
    expect(left.event_ticker).toBe('KXLIVE');
  });

  test('an incremental pass keeps rows it did not touch', () => {
    // Passing 0 disables staleness eviction: an incremental delta legitimately
    // touches a small fraction of the index, so "not seen" means nothing here.
    upsertIndexEvents(db, [event('KXA'), event('KXB')]);
    expect(pruneStaleEvents(db, 0)).toBe(0);
    expect(countIndexRows(db)).toBe(2);
  });

  test('events with no tradeable market left are dropped', () => {
    upsertIndexEvents(db, [
      event('KXCLOSED', [market({ ticker: 'KXCLOSED-T1', status: 'closed' })]),
      event('KXOPEN'),
    ]);
    expect(pruneStaleEvents(db, 0)).toBe(1);
    const left = db.query('SELECT event_ticker FROM event_index').get() as { event_ticker: string };
    expect(left.event_ticker).toBe('KXOPEN');
  });

  test('a settled market is not tradeable either', () => {
    upsertIndexEvents(db, [event('KXSETTLED', [market({ ticker: 'KXSETTLED-T1', result: 'yes' })])]);
    expect(pruneStaleEvents(db, 0)).toBe(1);
    expect(countIndexRows(db)).toBe(0);
  });

  test('an event past its close time is dropped', () => {
    upsertIndexEvents(db, [event('KXPAST', [market({ ticker: 'KXPAST-T1', close_time: '2020-01-01T00:00:00Z' })])]);
    expect(pruneStaleEvents(db, 0)).toBe(1);
    expect(countIndexRows(db)).toBe(0);
  });
});

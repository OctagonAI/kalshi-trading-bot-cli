import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import type { Database } from 'bun:sqlite';
import { getDb, closeDb } from '../../../db/index.js';
import { getLastFullRefresh, getLastRefresh, setLastFullRefresh, setLastRefresh } from '../../../db/event-index.js';
import { ensureIndex, forceRefreshIndex, getRefreshPromise } from '../search-index.js';

/**
 * Drives refreshIndex end to end against a fake Kalshi API. `events` answers
 * /events one page at a time; `alwaysMore` keeps handing back a cursor so the
 * walk hits the page cap. `seriesTags` answers /series/{ticker}. `eventsUrls`
 * records every /events request, so a test can tell a full walk from an
 * incremental one by its min_updated_ts.
 */
let alwaysMore = false;
let eventsUrls: URL[] = [];
let seriesTags: string[] = [];
let page = 0;

function fakeKalshi(input: Parameters<typeof fetch>[0]): Response {
  const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);
  if (url.pathname.endsWith('/events')) {
    eventsUrls.push(url);
    page++;
    const body = {
      events: [{ event_ticker: `EV-${page}`, series_ticker: 'SER', title: `Event ${page}`, category: 'Crypto', markets: [] }],
      cursor: alwaysMore ? `c${page}` : '',
    };
    return Response.json(body);
  }
  if (url.pathname.includes('/series/')) {
    return Response.json({ series: { ticker: 'SER', tags: seriesTags } });
  }
  return new Response('not found', { status: 404 });
}

describe('refreshIndex', () => {
  let db: Database;
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    closeDb();
    db = getDb(':memory:');
    alwaysMore = false;
    eventsUrls = [];
    seriesTags = [];
    page = 0;
    originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: Parameters<typeof fetch>[0]) => fakeKalshi(input)) as typeof fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    closeDb();
  });

  test('a complete walk records the refresh time', async () => {
    await forceRefreshIndex();
    expect(getLastRefresh(db)).not.toBeNull();
  });

  test('a walk cut off at the page cap keeps the previous refresh time', async () => {
    // Advancing it would let the next incremental pass start after events the
    // truncated walk never reached.
    setLastRefresh(db, 1_000);
    alwaysMore = true;
    await forceRefreshIndex();
    expect(getLastRefresh(db)).toBe(1_000);
  });

  test('a series whose tags were removed loses its stale tags', async () => {
    seriesTags = ['Bitcoin'];
    await forceRefreshIndex();
    expect(db.query("SELECT tags FROM event_index WHERE series_ticker = 'SER'").get()).toEqual({ tags: 'Bitcoin' });

    seriesTags = [];
    page = 0;
    await forceRefreshIndex();
    expect(db.query("SELECT tags FROM event_index WHERE series_ticker = 'SER'").get()).toEqual({ tags: '' });
  });

  test('only a complete full walk records the full-rebuild time', async () => {
    await forceRefreshIndex();
    const full = getLastFullRefresh(db);
    expect(full).not.toBeNull();

    // A truncated full walk never reached the tail, so it is not a full sweep.
    setLastFullRefresh(db, 1_000);
    alwaysMore = true;
    await forceRefreshIndex();
    expect(getLastFullRefresh(db)).toBe(1_000);
  });

  describe('routine refresh via ensureIndex', () => {
    const HOUR = 60 * 60 * 1000;

    /** Returns the stamps it set, before the refresh ran. */
    async function refreshStaleIndex(lastFullAgo: number): Promise<{ lastRefresh: number; lastFull: number }> {
      await forceRefreshIndex(); // populate the index
      eventsUrls = [];
      page = 0;
      const now = Date.now();
      // Old enough that ensureIndex starts a refresh.
      const stamps = { lastRefresh: now - 3 * HOUR, lastFull: now - lastFullAgo };
      setLastRefresh(db, stamps.lastRefresh);
      setLastFullRefresh(db, stamps.lastFull);
      await ensureIndex();
      await getRefreshPromise();
      return stamps;
    }

    test('runs incrementally while the last full rebuild is inside the TTL', async () => {
      const stamps = await refreshStaleIndex(3 * HOUR);
      expect(eventsUrls[0].searchParams.has('min_updated_ts')).toBe(true);
      // The incremental pass advances last_refresh but not the full-rebuild time.
      expect(getLastRefresh(db)!).toBeGreaterThan(stamps.lastRefresh);
      expect(getLastFullRefresh(db)).toBe(stamps.lastFull);
    });

    test('rebuilds in full once the last full rebuild ages past the TTL, however recent the last refresh', async () => {
      const stamps = await refreshStaleIndex(25 * HOUR);
      expect(eventsUrls[0].searchParams.has('min_updated_ts')).toBe(false);
      expect(getLastFullRefresh(db)!).toBeGreaterThan(stamps.lastFull);
    });
  });
});

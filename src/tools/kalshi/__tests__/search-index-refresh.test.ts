import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import type { Database } from 'bun:sqlite';
import { getDb, closeDb } from '../../../db/index.js';
import { getLastRefresh } from '../../../db/event-index.js';
import { forceRefreshIndex } from '../search-index.js';

/**
 * Drives refreshIndex end to end against a fake Kalshi API: /events returns
 * one page, and `seriesTags` answers /series/{ticker}.
 */
let seriesTags: string[] = [];
let page = 0;

function fakeKalshi(input: Parameters<typeof fetch>[0]): Response {
  const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);
  if (url.pathname.endsWith('/events')) {
    page++;
    const body = {
      events: [{ event_ticker: `EV-${page}`, series_ticker: 'SER', title: `Event ${page}`, category: 'Crypto', markets: [] }],
      cursor: '',
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


  test('a series whose tags were removed loses its stale tags', async () => {
    seriesTags = ['Bitcoin'];
    await forceRefreshIndex();
    expect(db.query("SELECT tags FROM event_index WHERE series_ticker = 'SER'").get()).toEqual({ tags: 'Bitcoin' });

    seriesTags = [];
    page = 0;
    await forceRefreshIndex();
    expect(db.query("SELECT tags FROM event_index WHERE series_ticker = 'SER'").get()).toEqual({ tags: '' });
  });
});

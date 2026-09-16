import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';
import { searchOctagonMarkets, type MarketSearchRow, type PagedResult } from '../../scan/octagon-kalshi-api.js';
import { contractOf, formatEventMarketsHuman } from '../search-remote.js';

function installFetchMock(capture: { url?: string }) {
  globalThis.fetch = mock(async (url: string | URL | Request) => {
    capture.url = typeof url === 'string' ? url : url instanceof URL ? url.toString() : url.url;
    return new Response(JSON.stringify({ data: [], next_cursor: null, has_more: false }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  }) as unknown as typeof fetch;
}

function market(over: Partial<MarketSearchRow>): MarketSearchRow {
  return {
    market_ticker: 'KXBTCD-26SEP1517-T76999.99',
    event_ticker: 'KXBTCD-26SEP1517',
    title: 'Bitcoin price on Sep 15, 2026?',
    ...over,
  } as MarketSearchRow;
}

function page(rows: MarketSearchRow[]): PagedResult<MarketSearchRow> {
  return { data: rows, next_cursor: null, has_more: false } as PagedResult<MarketSearchRow>;
}

describe('searchOctagonMarkets', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    process.env.OCTAGON_API_KEY = 'sk_test_key';
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    delete process.env.OCTAGON_API_KEY;
  });

  test('drills into an event on the venue-agnostic route, pinned to one venue', async () => {
    const capture: { url?: string } = {};
    installFetchMock(capture);
    await searchOctagonMarkets({ event_ticker: 'KXBTCD-26SEP1517', limit: 30 });

    expect(capture.url).toContain('/v1/predictions/markets/search');
    expect(capture.url).toContain('venues=kalshi');
    expect(capture.url).toContain('event_ticker=KXBTCD-26SEP1517');
  });

  test('unrecognised params are dropped rather than forwarded', async () => {
    // This route answers an unknown parameter with 200 and zero rows, which is
    // indistinguishable from "no results" — so anything off the whitelist must
    // never reach the wire.
    const capture: { url?: string } = {};
    installFetchMock(capture);
    await searchOctagonMarkets({
      event_ticker: 'KXBTCD-26SEP1517',
      meta_category: 'Crypto',
      report: 'ready',
    } as unknown as Parameters<typeof searchOctagonMarkets>[0]);

    expect(capture.url).not.toContain('meta_category');
    expect(capture.url).not.toContain('report');
  });

  test('market filters do reach the wire — they are what this route is for', async () => {
    const capture: { url?: string } = {};
    installFetchMock(capture);
    await searchOctagonMarkets({ q: 'bitcoin', min_volume_24h: 1000, sort_by: 'volume_24h' });

    expect(capture.url).toContain('min_volume_24h=1000');
    expect(capture.url).toContain('sort_by=volume_24h');
  });
});

describe('formatEventMarketsHuman', () => {
  test('a Kalshi ladder is labelled by strike, not by its one shared title', () => {
    const out = formatEventMarketsHuman('KXBTCD-26SEP1517', page([
      market({ market_ticker: 'KXBTCD-26SEP1517-T77249.99', yes_subtitle: '$77,250 or above' }),
      market({ market_ticker: 'KXBTCD-26SEP1517-T76999.99', yes_subtitle: '$77,000 or above' }),
    ]));

    expect(out).toContain('Strike');
    expect(out).toContain('$77,000 or above');
    // The shared title is not what identifies a row here
    expect(out).not.toContain('Outcome');
    // Event prefix stripped: the strike survives instead of being truncated away
    expect(out).toContain('T76999.99');
    expect(out).not.toContain('KXBTCD-26SEP1517-T76999.99');
  });

  test('rows are sorted, since the API returns a ladder unordered', () => {
    const out = formatEventMarketsHuman('KXBTCD-26SEP1517', page([
      market({ market_ticker: 'KXBTCD-26SEP1517-T77749.99', yes_subtitle: '$77,750 or above' }),
      market({ market_ticker: 'KXBTCD-26SEP1517-T76999.99', yes_subtitle: '$77,000 or above' }),
      market({ market_ticker: 'KXBTCD-26SEP1517-T77499.99', yes_subtitle: '$77,500 or above' }),
    ]));
    const body = out.slice(out.indexOf('T76999.99'));
    expect(body.indexOf('T76999.99')).toBeLessThan(body.indexOf('T77499.99'));
    expect(body.indexOf('T77499.99')).toBeLessThan(body.indexOf('T77749.99'));
  });

  test('a Polymarket-shaped event is labelled by outcome, the field that varies there', () => {
    // Mirror image of Kalshi: yes_subtitle is "Yes" on every row and the title
    // carries the outcome.
    const out = formatEventMarketsHuman('polymarket__nobel-peace-prize-winner-2026-139', page([
      market({ market_ticker: 'will-trump-win-123', title: 'Donald Trump', yes_subtitle: 'Yes' }),
      market({ market_ticker: 'will-guterres-win-456', title: 'António Guterres', yes_subtitle: 'Yes' }),
    ]));

    expect(out).toContain('Outcome');
    expect(out).toContain('Donald Trump');
    expect(out).not.toContain('Strike');
  });

  test('an empty result says so plainly, without guessing why', () => {
    const out = formatEventMarketsHuman('KXBTCD-33APR0610', page([]));
    expect(out).toContain('No markets found for KXBTCD-33APR0610.');
  });
});

describe('contractOf', () => {
  test('strips the event prefix', () => {
    expect(contractOf('KXBTCD-33APR0610-T59599.99', 'KXBTCD-33APR0610')).toBe('T59599.99');
  });

  test('leaves a ticker that does not carry the prefix alone', () => {
    // KXELONMARS-99's only market IS KXELONMARS-99 — nothing to strip.
    expect(contractOf('KXELONMARS-99', 'KXELONMARS-99')).toBe('KXELONMARS-99');
    expect(contractOf('will-trump-win-123', 'nobel-peace-prize-winner-2026')).toBe('will-trump-win-123');
  });
});

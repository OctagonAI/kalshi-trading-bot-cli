import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';
import { searchOctagonEvents } from '../octagon-kalshi-api.js';

/**
 * These pin the two ways this endpoint fails SILENTLY — it answers a bad
 * request with zero rows rather than an error, so both look identical to
 * "nothing matched":
 *
 *   1. an unrecognised query parameter empties the result set
 *   2. meta_category is case-sensitive, so a lowercased value returns nothing
 */
describe('searchOctagonEvents', () => {
  let originalFetch: typeof globalThis.fetch;
  let lastUrl = '';

  beforeEach(() => {
    originalFetch = globalThis.fetch;
    globalThis.fetch = mock(async (url: string | URL | Request) => {
      lastUrl = typeof url === 'string' ? url : url instanceof URL ? url.toString() : url.url;
      return new Response(JSON.stringify({ data: [], next_cursor: null, has_more: false }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    }) as unknown as typeof fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test('hits the venue-agnostic route and forces venues=kalshi', async () => {
    await searchOctagonEvents({ q: 'bitcoin', limit: 30 });
    const url = new URL(lastUrl);
    expect(url.origin + url.pathname).toBe('https://api.octagonai.co/v1/predictions/markets/events/search');
    expect(url.searchParams.get('venues')).toBe('kalshi');
    expect(url.searchParams.get('q')).toBe('bitcoin');
    expect(url.searchParams.get('limit')).toBe('30');
  });

  test('passes meta_category through verbatim, ampersand and case intact', async () => {
    await searchOctagonEvents({ meta_category: 'Tech & Science', limit: 10 });
    const url = new URL(lastUrl);
    expect(url.searchParams.get('meta_category')).toBe('Tech & Science');
  });

  test('drops parameters this endpoint does not understand', async () => {
    // Forwarding any of these would silently return zero rows.
    await searchOctagonEvents({
      q: 'shutdown',
      sort_by: 'volume_24h',
      min_volume_24h: 1000,
      close_before: '2026-12-31',
      category: 'Crypto',
    } as unknown as Parameters<typeof searchOctagonEvents>[0]);
    const url = new URL(lastUrl);
    expect(url.searchParams.get('q')).toBe('shutdown');
    expect(url.searchParams.get('sort_by')).toBeNull();
    expect(url.searchParams.get('min_volume_24h')).toBeNull();
    expect(url.searchParams.get('close_before')).toBeNull();
    expect(url.searchParams.get('category')).toBeNull();
  });

  test('omits keys that were not supplied', async () => {
    await searchOctagonEvents({ meta_category: 'Crypto' });
    const url = new URL(lastUrl);
    expect(url.searchParams.get('q')).toBeNull();
    expect(url.searchParams.get('cursor')).toBeNull();
    expect(url.searchParams.get('meta_category')).toBe('Crypto');
  });
});

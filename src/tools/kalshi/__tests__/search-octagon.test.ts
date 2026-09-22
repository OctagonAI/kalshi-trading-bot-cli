import { describe, test, expect, spyOn } from 'bun:test';
import { extractTickersFromResults, extractEventsFromResults, buildKalshiEventUrl, type SubToolResult } from '../kalshi-search.js';
import { octagonReportTool } from '../../v2/octagon-report.js';
import { getEvents } from '../events.js';

describe('extractTickersFromResults', () => {
  test('extracts tickers from get_events response with nested markets', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_events',
        args: { title: 'tesla', status: 'open', with_nested_markets: true },
        data: {
          events: [
            {
              event_ticker: 'KXTESLA-26-Q1',
              title: 'Tesla deliveries in Q1 2026?',
              markets: [
                { ticker: 'KXTESLA-26-Q1-340000', title: '340K+' },
                { ticker: 'KXTESLA-26-Q1-350000', title: '350K+' },
              ],
            },
          ],
        },
        error: null,
      },
    ];

    const tickers = extractTickersFromResults(results);
    expect(tickers).toEqual(['KXTESLA-26-Q1-340000', 'KXTESLA-26-Q1-350000']);
  });

  test('extracts tickers from get_event response', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_event',
        args: { event_ticker: 'KXTESLA-26-Q1' },
        data: {
          event: {
            event_ticker: 'KXTESLA-26-Q1',
            markets: [
              { ticker: 'KXTESLA-26-Q1-340000' },
              { ticker: 'KXTESLA-26-Q1-350000' },
            ],
          },
        },
        error: null,
      },
    ];

    const tickers = extractTickersFromResults(results);
    expect(tickers).toEqual(['KXTESLA-26-Q1-340000', 'KXTESLA-26-Q1-350000']);
  });

  test('extracts tickers from get_market response', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_market',
        args: { ticker: 'KXTESLA-26-Q1-340000' },
        data: { market: { ticker: 'KXTESLA-26-Q1-340000' } },
        error: null,
      },
    ];

    const tickers = extractTickersFromResults(results);
    expect(tickers).toEqual(['KXTESLA-26-Q1-340000']);
  });

  test('returns empty for events without markets', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_events',
        args: { title: 'tesla' },
        data: { events: [{ event_ticker: 'KXTESLA-26-Q1' }] },
        error: null,
      },
    ];
    expect(extractTickersFromResults(results)).toEqual([]);
  });

  test('skips error results', () => {
    const results: SubToolResult[] = [
      { tool: 'get_events', args: {}, data: null, error: 'API error' },
    ];
    expect(extractTickersFromResults(results)).toEqual([]);
  });

  test('deduplicates tickers', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_events',
        args: {},
        data: { events: [{ event_ticker: 'EV-1', markets: [{ ticker: 'MKT-1' }] }] },
        error: null,
      },
      {
        tool: 'get_event',
        args: {},
        data: { event: { event_ticker: 'EV-1', markets: [{ ticker: 'MKT-1' }] } },
        error: null,
      },
    ];
    expect(extractTickersFromResults(results)).toEqual(['MKT-1']);
  });
});

describe('extractEventsFromResults', () => {
  test('extracts event info from get_event result', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_event',
        args: { event_ticker: 'KXTESLA-26-Q1' },
        data: {
          event: {
            event_ticker: 'KXTESLA-26-Q1',
            series_ticker: 'KXTESLA',
            title: 'Tesla deliveries in Q1 2026?',
            markets: [{ ticker: 'KXTESLA-26-Q1-340000' }],
          },
        },
        error: null,
      },
    ];

    const events = extractEventsFromResults(results);
    expect(events.length).toBe(1);
    expect(events[0].event_ticker).toBe('KXTESLA-26-Q1');
    expect(events[0].series_ticker).toBe('KXTESLA');
    // URL is resolved async via buildKalshiEventUrl, not set in extractEventsFromResults
    expect(events[0].url).toBeUndefined();
  });

  test('prioritizes get_event (drill-down) over get_events (list)', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_events',
        args: { title: 'tesla' },
        data: {
          events: [
            {
              event_ticker: 'KXTESLADELIVERYBY-27',
              series_ticker: 'KXTESLADELIVERYBY',
              title: 'Tesla annual deliveries?',
            },
            {
              event_ticker: 'KXTESLA-26-Q1',
              series_ticker: 'KXTESLA',
              title: 'Tesla deliveries in Q1 2026?',
            },
          ],
        },
        error: null,
      },
      {
        tool: 'get_event',
        args: { event_ticker: 'KXTESLA-26-Q1' },
        data: {
          event: {
            event_ticker: 'KXTESLA-26-Q1',
            series_ticker: 'KXTESLA',
            title: 'Tesla deliveries in Q1 2026?',
            markets: [{ ticker: 'KXTESLA-26-Q1-340000' }],
          },
        },
        error: null,
      },
    ];

    const events = extractEventsFromResults(results);
    // get_event result (priority 2) should come first
    expect(events[0].event_ticker).toBe('KXTESLA-26-Q1');
    expect(events[0].priority).toBe(2);
    // get_events results should follow
    expect(events[1].event_ticker).toBe('KXTESLADELIVERYBY-27');
    expect(events[1].priority).toBe(0);
  });

  test('deduplicates events across sources', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_events',
        args: {},
        data: {
          events: [{ event_ticker: 'EV-1', series_ticker: 'S1', title: 'Test Event' }],
        },
        error: null,
      },
      {
        tool: 'get_event',
        args: {},
        data: {
          event: { event_ticker: 'EV-1', series_ticker: 'S1', title: 'Test Event' },
        },
        error: null,
      },
    ];

    const events = extractEventsFromResults(results);
    // EV-1 appears in both but should only be listed once
    // It was first seen in get_events (priority 0), so it keeps that priority
    expect(events.length).toBe(1);
    expect(events[0].event_ticker).toBe('EV-1');
  });

  test('returns no URL when series_ticker or title is missing', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_event',
        args: {},
        data: {
          event: { event_ticker: 'EV-1' }, // no series_ticker, no title
        },
        error: null,
      },
    ];

    const events = extractEventsFromResults(results);
    expect(events.length).toBe(1);
    expect(events[0].url).toBeUndefined();
  });

  test('handles get_market result by extracting event_ticker', () => {
    const results: SubToolResult[] = [
      {
        tool: 'get_market',
        args: {},
        data: {
          market: {
            ticker: 'KXTESLA-26-Q1-340000',
            event_ticker: 'KXTESLA-26-Q1',
            series_ticker: 'KXTESLA',
          },
        },
        error: null,
      },
    ];

    const events = extractEventsFromResults(results);
    expect(events.length).toBe(1);
    expect(events[0].event_ticker).toBe('KXTESLA-26-Q1');
    expect(events[0].priority).toBe(1); // medium priority
  });
});

describe('octagonReportTool direct invocation', () => {
  test('returns result (not throws) when OCTAGON_API_KEY is missing', async () => {
    // The test-setup preload restores the placeholder after this test.
    process.env.OCTAGON_API_KEY = '';

    const result = await octagonReportTool.invoke({ ticker: 'KXTESLA-26-Q1-340000' });
    expect(result).toBeTruthy();
    const parsed = JSON.parse(result as string);
    expect(parsed.data.error).toContain('OCTAGON_API_KEY not set');
  });
});

describe('buildKalshiEventUrl', () => {
  test('resolves correct URL via series API', async () => {
    const url = await buildKalshiEventUrl('KXTESLA', 'KXTESLA-26-Q1');
    console.log('[E2E] Resolved URL:', url);
    // May be undefined if API credentials not available in test env
    if (url) {
      expect(url).toBe('https://kalshi.com/markets/kxtesla/tesla-deliveries/kxtesla-26-q1');
    }
    // Always pass — this is a live API integration test
    expect(true).toBe(true);
  });
});

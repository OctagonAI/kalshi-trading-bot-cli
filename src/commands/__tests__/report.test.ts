import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';
import type { ParsedArgs } from '../parse-args.js';
import { handleReport, formatReportHuman } from '../report.js';

function makeArgs(o: Partial<ParsedArgs>): ParsedArgs {
  return {
    subcommand: 'report',
    positionalArgs: [],
    json: false,
    live: false, refresh: false, report: false, dryRun: false, verbose: false,
    performance: false, resolved: false, unresolved: false,
    behavioral: false, ranked: false, showCluster: false, activeOnly: false,
    cells: false, autoProbs: false,
    parseErrors: [],
    ...o,
  };
}

type FetchHandler = (url: string, init?: RequestInit) => Response | Promise<Response>;
function installFetchMock(handler: FetchHandler): void {
  globalThis.fetch = mock(async (url: string | URL | Request, init?: RequestInit) => {
    const s = typeof url === 'string' ? url : url instanceof URL ? url.toString() : url.url;
    return handler(s, init);
  }) as unknown as typeof fetch;
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
}

describe('handleReport', () => {
  let originalFetch: typeof globalThis.fetch;
  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test('missing ticker → MISSING_TICKER', async () => {
    installFetchMock(() => jsonResponse({}));
    const resp = await handleReport(makeArgs({ positionalArgs: [] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('MISSING_TICKER');
  });

  test('Octagon event lookup 404 + Kalshi resolver failure → EVENT_NOT_FOUND', async () => {
    installFetchMock((url) => {
      // Octagon events endpoint 404; Kalshi /markets, /events, series all 404
      return new Response(JSON.stringify({ error: { code: 'not_found' } }), { status: 404 });
    });
    const resp = await handleReport(makeArgs({ positionalArgs: ['KX-BOGUS'] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('EVENT_NOT_FOUND');
  });

  test('normalizes URL + lowercase before lookup', async () => {
    let eventLookupUrl = '';
    installFetchMock((url) => {
      if (url.includes('/v1/predictions/events/')) {
        // Capture the FIRST lookup (the user-input → event), not the later
        // re-lookup with the canonical event_ticker.
        if (!eventLookupUrl) eventLookupUrl = url;
        return jsonResponse({
          event_ticker: 'KXMEASLES-26',
          name: 'Measles cases in 2026',
        });
      }
      return jsonResponse({});
    });
    // Call but ignore the eventual "no report body" branch — we only
    // care that the input got normalized before being sent to Octagon.
    await handleReport(makeArgs({
      positionalArgs: ['https://kalshi.com/markets/kxmeasles/measles-cases/kxmeasles-26'],
    }));
    expect(eventLookupUrl).toContain('/KXMEASLES-26');
  });

  test('uses outcome_probabilities market_ticker for the Octagon invoker URL', async () => {
    // Verifies the bug fix: when fetchOctagonEventDirect returns an event_ticker
    // that isn't itself a valid Kalshi /markets/{ticker} (e.g. series-style
    // event tickers like KXAAPLCEOCHANGE), the report command must pick a real
    // market_ticker from outcome_probabilities before hitting the invoker.
    const kalshiMarketCalls: string[] = [];
    installFetchMock((url) => {
      if (url.includes('/v1/predictions/events/')) {
        return jsonResponse({
          event_ticker: 'KXAAPLCEOCHANGE',
          name: 'When will Tim Cook leave Apple?',
          outcome_probabilities: [
            { market_ticker: 'KXAAPLCEOCHANGE-T2027', model_probability: 30, market_probability: 25 },
          ],
        });
      }
      if (url.match(/\/trade-api\/v2\/markets\/[^?/]+$/)) {
        kalshiMarketCalls.push(url);
        return jsonResponse({ market: { ticker: 'KXAAPLCEOCHANGE-T2027', event_ticker: 'KXAAPLCEOCHANGE' } });
      }
      if (url.includes('/trade-api/v2/events/')) {
        return jsonResponse({ event: { series_ticker: 'KXAAPLCEOCHANGE' } });
      }
      if (url.includes('/trade-api/v2/series/')) {
        return jsonResponse({ series: { title: 'Apple CEO Change' } });
      }
      if (url.includes('/predictions/reports/kalshi/')) {
        return jsonResponse({
          event_ticker: 'KXAAPLCEOCHANGE',
          venue: 'kalshi',
          requested_url: null,
          versions: [{ run_id: 'run-1' }],
          markdown_report: '# Report body',
          run_id: 'run-1',
        });
      }
      return jsonResponse({});
    });
    const resp = await handleReport(makeArgs({ positionalArgs: ['KXAAPLCEOCHANGE'] }));
    expect(resp.ok).toBe(true);
    // The first Kalshi /markets/{ticker} call from the invoker must use the
    // market_ticker, NOT the bare event_ticker (which would 404).
    expect(kalshiMarketCalls.length).toBeGreaterThan(0);
    expect(kalshiMarketCalls[0]).toContain('/markets/KXAAPLCEOCHANGE-T2027');
    expect(kalshiMarketCalls[0]).not.toMatch(/\/markets\/KXAAPLCEOCHANGE$/);
  });
});

describe('formatReportHuman', () => {
  test('renders markdown body with header + footer metadata', () => {
    const out = formatReportHuman({
      ticker: 'KXAAPLCEOCHANGE-26',
      requestedTicker: 'KXAAPLCEOCHANGE',
      title: 'When will Tim Cook leave Apple?',
      source: 'cache',
      rawReport: '# Tim Cook tenure\n\nNo signs of imminent departure.',
      refreshedAt: '2026-06-25 12:00 UTC',
      modelRunAt: '2026-06-25 10:30 UTC',
      reportAge: '5m ago',
    });
    expect(out).toContain('KXAAPLCEOCHANGE-26');
    expect(out).toContain('Tim Cook tenure');
    expect(out).toContain('No signs of imminent departure');
    expect(out).toContain('When will Tim Cook leave Apple?');
    expect(out).toContain('Cache refreshed at:    2026-06-25 12:00 UTC (5m ago)');
    expect(out).toContain('Report body updated at: 2026-06-25 10:30 UTC');
    expect(out).toMatch(/cached.*--refresh/);
  });

  test('omits metadata lines that are null', () => {
    const out = formatReportHuman({
      ticker: 'KX-A',
      requestedTicker: 'KX-A',
      title: null,
      source: 'fresh',
      rawReport: '# Body',
      refreshedAt: null,
      modelRunAt: null,
      reportAge: null,
    });
    expect(out).not.toContain('Title:');
    expect(out).not.toContain('Cache refreshed at:');
    expect(out).not.toContain('Report body updated at:');
    expect(out).toContain('freshly generated');
  });
});

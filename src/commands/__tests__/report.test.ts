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

// Other suites in the project unset KALSHI_PRIVATE_KEY in their afterEach
// (see hardening.test.ts). Some report paths flow into the Octagon invoker,
// which signs Kalshi requests — so the report suite must restore credentials
// in its own beforeEach to stay order-independent.
const TEST_PRIVATE_KEY = [
  '-----BEGIN PRIVATE KEY-----',
  'MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCRFVyyjP3KGX63',
  '0/qa6kWsCdNJTbKMBaqTaYzCVKYWr3fA4UcA3Wx9+mXwYQ0+jULQP9Y1qWBpWTmb',
  'vnZaejJaywFK6LESStChcXuqN8uBcF13+CfwxVdbTboAbaHaNsOjHwl6JuYW0Nz+',
  'jOQmN0v/nT/SSq8BOLN7S408VW5yR3sC+W9oJ0qb6gVNJTHazxuEvCjz8k5w+a+D',
  'otAVUg/Y9WVIJqKhIhvQnD2pAN5J20RI4YXfz31GTaKzwMmg/ByoGrtkeJw4StFW',
  'HSVfo2/j9H1EdMTEHyjLyGyXjfiQOTSp/gK0BjaMHGzdltFueCOss8RoQjv2n+2m',
  'OL+aNv7tAgMBAAECggEAEkm0DpmxH/mIvJlO3JotQBtY88OEfxvzvXMvmAtdiDyE',
  'Bt8euSAwHc0jbmJ9beYWhvOVB9ya14y0s0oV1x/SGxm9xvh/4YNmuwL4CKPR1jYY',
  'wheYyUPG2C57BLTNExmWHYi7BBfFJxka0kdmNt7/iHAE7HgXiTrhfOgwHGvUaTki',
  'zDuq/I2rUaG4bDHA8EK19DdFCb2+TuqGYnc7vkMgwz2NajGZNXqOWCJabMVLeQR2',
  'niVRsFo2kY1uXB6Oy+nEixVnTxWRQhT//UWbLr4iJZnlJGpwPGKZZHhNADbx+w+0',
  'ig3iqVnYY11s7cceGTV7C9fGr+H9pERtTp3e1cPmLQKBgQDIP2WoJVz12wUd4ANM',
  'Jz1xpxsYg3txnTST01OidaWxeaDHg/mjzsdKPdMa7eBREJYy4HUllLZrvI9KWp/4',
  'wLCB0aCuytGf6Z2u/bOoTs87HMf13PzC0ksD1Ri9wEECN5NlVnL9NNcnpPE+6gGY',
  '2OzJtzfdr5JwPC5U12IDQVEAWwKBgQC5eiZhZKwHHeQQzJqgURDd3hZJpQdFDcFp',
  'QSH1dNHNdNutTLZ7JakSQcoz9P4Fuu4AEPGCi94xH4NoIq7fPY4ABX0a3vp9guJ+',
  'txChCHusjwVGGcraGSiognyxBnewpt+lzv1xDWBmmGaDqSVayS9eQaEiMypHbaah',
  '2vsiQBWgVwKBgC/EN6qZZwhae2j5869puNVwiB0b2Als94q/oTaim6ivG7Qb/iOe',
  'ApnqD35f+d88dqeiNS+GvtEKRJ/26Cv9Qt1ktNCdHs3ney6v4/gk/HfcULKMSVrr',
  'sOs0HNe+kYNG4IkOyxUtUplpVgas6T6dmDYx10ixRdwx7tdcHUwre3f7AoGARkWP',
  'UQsRWkjq5ap/Uwojt8uy6ggKbxE9HCG/Of4elxcVO916rcGhAvfGIlVKAOXH0mKY',
  '/fr8HeRwpv2s/4uUx1FNCuc8RF1YbuXw+PH72W7+cobHIkax7tYxY+itZFJ1HZ8E',
  'ytZklbpb7LojGvhqZ+25nPmBpTpYDa6nw1xAVVUCgYEAqKcg/QSJIcj+qODjtZZ8',
  'aCqNvagzw74Hruh9jmd3tLvqpzKN72GqdtuzRoGi2BzmjUkrTXhEugf4/AaxfLMy',
  'yk6j0nzHRSVi1GUzx/P/q6gsR8bEvhhBSZEwQxcQDL+1Toamz1nmFXLZo0w3hi6q',
  'wZ0ONbXRO/Hcg1MzeK10biQ=',
  '-----END PRIVATE KEY-----',
].join('\n');

describe('handleReport', () => {
  let originalFetch: typeof globalThis.fetch;
  beforeEach(() => {
    process.env.OCTAGON_API_KEY = 'sk_test';
    process.env.KALSHI_API_KEY = 'test-key';
    process.env.KALSHI_PRIVATE_KEY = TEST_PRIVATE_KEY;
    originalFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = originalFetch;
    delete process.env.OCTAGON_API_KEY;
    delete process.env.KALSHI_API_KEY;
    delete process.env.KALSHI_PRIVATE_KEY;
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

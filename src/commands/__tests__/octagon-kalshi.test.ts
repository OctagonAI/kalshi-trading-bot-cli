import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';
import type { ParsedArgs } from '../parse-args.js';
import { handleSimilar } from '../similar.js';
import { handleClusters } from '../clusters.js';
import { handlePeers } from '../peers.js';
import { handleCorrelate } from '../correlate.js';
import { handleBasket } from '../basket.js';

function makeArgs(overrides: Partial<ParsedArgs>): ParsedArgs {
  return {
    subcommand: 'chat',
    positionalArgs: [],
    json: false,
    live: false,
    refresh: false,
    report: false,
    dryRun: false,
    verbose: false,
    performance: false,
    resolved: false,
    unresolved: false,
    behavioral: false,
    ranked: false,
    showCluster: false,
    parseErrors: [],
    ...overrides,
  };
}

type FetchHandler = (url: string, init?: RequestInit) => Response | Promise<Response>;

function installFetchMock(handler: FetchHandler) {
  globalThis.fetch = mock(async (url: string | URL | Request, init?: RequestInit) => {
    const urlStr = typeof url === 'string' ? url : url instanceof URL ? url.toString() : url.url;
    return handler(urlStr, init);
  }) as unknown as typeof fetch;
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } });
}

describe('Octagon Kalshi commands', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    process.env.OCTAGON_API_KEY = 'sk_test_key';
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    delete process.env.OCTAGON_API_KEY;
  });

  test('handleSimilar: ticker anchor', async () => {
    installFetchMock((url) => {
      expect(url).toContain('/markets/similar');
      expect(url).toContain('anchor_ticker=KXBTCD-26DEC31-T100000');
      return jsonResponse({
        anchor_ticker: 'KXBTCD-26DEC31-T100000',
        anchor_query: null,
        data: [{
          market_ticker: 'KXETHU-26DEC31-T10000',
          event_ticker: 'KXETHU-26DEC31',
          title: 'ETH above $10k by Dec 2026',
          status: 'active',
          close_time: '2026-12-31T23:59:59Z',
          category: 'crypto',
          distance: 0.18,
        }],
      });
    });

    const resp = await handleSimilar(makeArgs({ subcommand: 'similar', positionalArgs: ['KXBTCD-26DEC31-T100000'], topK: 5 }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    expect(resp.data.data).toHaveLength(1);
    expect(resp.data.data[0].distance).toBe(0.18);
  });

  test('handleSimilar: free-text query routed to -q', async () => {
    installFetchMock((url) => {
      expect(url).toContain('/markets/similar');
      expect(url).toMatch(/q=[^&]*Bitcoin/);
      return jsonResponse({ anchor_ticker: null, anchor_query: 'Will Bitcoin pierce six figures', data: [] });
    });
    const resp = await handleSimilar(makeArgs({ subcommand: 'similar', query: 'Will Bitcoin pierce six figures' }));
    expect(resp.ok).toBe(true);
  });

  test('handleSimilar: rejects missing anchor', async () => {
    installFetchMock(() => jsonResponse({}));
    const resp = await handleSimilar(makeArgs({ subcommand: 'similar' }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('MISSING_ANCHOR');
  });

  test('handleClusters: list', async () => {
    installFetchMock((url) => {
      expect(url).toContain('/clusters');
      return jsonResponse({ data: [{ cluster_id: 1, label: 'Fed decisions', description: 'FOMC', size: 14, sample_titles: ['?'], created_at: 'now' }] });
    });
    const resp = await handleClusters(makeArgs({ subcommand: 'clusters', labelContains: 'fed' }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    expect(resp.data.kind).toBe('list');
  });

  test('handleClusters: members by id', async () => {
    installFetchMock((url) => {
      expect(url).toContain('/clusters/42/markets');
      return jsonResponse({ data: [], next_cursor: null, has_more: false });
    });
    const resp = await handleClusters(makeArgs({ subcommand: 'clusters', positionalArgs: ['42'] }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    expect(resp.data.kind).toBe('members');
  });

  test('handleClusters: ranked', async () => {
    installFetchMock((url) => {
      expect(url).toContain('/clusters/ranked-by-return');
      return jsonResponse({ timeframe: '1y', kind: 'thematic', top_n_per_cluster: 5, min_return: 0.2, data: [] });
    });
    const resp = await handleClusters(makeArgs({ subcommand: 'clusters', ranked: true, timeframe: '1y', minReturn: 0.2 }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    expect(resp.data.kind).toBe('ranked');
  });

  test('handlePeers: thematic peers', async () => {
    installFetchMock((url) => {
      expect(url).toContain('/markets/');
      expect(url).toContain('/cluster-peers');
      return jsonResponse({
        market_ticker: 'KX-A',
        kind: 'thematic',
        cluster: { cluster_id: 1, label: 'l', description: 'd', size: 10 },
        data: [],
      });
    });
    const resp = await handlePeers(makeArgs({ subcommand: 'peers', positionalArgs: ['KX-A'] }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    expect(resp.data.kind).toBe('peers');
  });

  test('handleCorrelate: rejects fewer than 2 tickers', async () => {
    installFetchMock(() => jsonResponse({}));
    const resp = await handleCorrelate(makeArgs({ subcommand: 'correlate', positionalArgs: ['KX-A'] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('TOO_FEW_TICKERS');
  });

  test('handleCorrelate: posts matrix request', async () => {
    installFetchMock(async (url, init) => {
      expect(url).toContain('/markets/correlations');
      const body = JSON.parse((init?.body as string) ?? '{}');
      expect(body.market_tickers).toEqual(['KX-A', 'KX-B']);
      expect(body.window_days).toBe(90);
      return jsonResponse({
        tickers: ['KX-A', 'KX-B'],
        matrix: [[1, 0.5], [0.5, 1]],
        ranked_pairs: [{ ticker_a: 'KX-A', ticker_b: 'KX-B', correlation: 0.5 }],
        window_days: 90,
        interval: '1d',
        missing: [],
      });
    });
    const resp = await handleCorrelate(makeArgs({ subcommand: 'correlate', positionalArgs: ['KX-A', 'KX-B'], windowDays: 90 }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    expect(resp.data.matrix).toEqual([[1, 0.5], [0.5, 1]]);
  });

  test('handleBasket build: equal sizing when no Kelly fields', async () => {
    installFetchMock(async (url, init) => {
      expect(url).toContain('/baskets/build');
      const body = JSON.parse((init?.body as string) ?? '{}');
      expect(body.sizing.strategy).toBe('equal');
      expect(body.universe.category).toBe('crypto');
      expect(body.n).toBe(5);
      return jsonResponse({
        legs: [],
        realized_max_pairwise_correlation: 0.0,
        cluster_breakdown: {},
        dropped: [],
        universe_size: 0,
      });
    });
    const resp = await handleBasket(makeArgs({ subcommand: 'basket', positionalArgs: ['build'], category: 'crypto', n: 5 }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    expect(resp.data.sub).toBe('build');
  });

  test('handleBasket build: kelly sizing when bankroll set', async () => {
    installFetchMock(async (url, init) => {
      const body = JSON.parse((init?.body as string) ?? '{}');
      expect(body.sizing.strategy).toBe('kelly');
      expect(body.sizing.bankroll_usd).toBe(1000);
      expect(body.sizing.kelly_multiplier).toBe(0.25);
      expect(body.sizing.leg_probabilities).toEqual({ 'KX-A': 0.62, 'KX-B': 0.58 });
      return jsonResponse({
        legs: [], realized_max_pairwise_correlation: 0, cluster_breakdown: {}, dropped: [], universe_size: 0,
      });
    });
    const resp = await handleBasket(makeArgs({
      subcommand: 'basket', positionalArgs: ['build'], category: 'crypto', n: 5,
      bankroll: 1000, kellyMultiplier: 0.25, probabilities: 'KX-A:0.62,KX-B:0.58',
    }));
    expect(resp.ok).toBe(true);
  });

  test('handleBasket backtest: tickers required', async () => {
    installFetchMock(() => jsonResponse({}));
    const resp = await handleBasket(makeArgs({ subcommand: 'basket', positionalArgs: ['backtest'] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('MISSING_TICKERS');
  });

  test('handleBasket size: probs required', async () => {
    installFetchMock(() => jsonResponse({}));
    const resp = await handleBasket(makeArgs({ subcommand: 'basket', positionalArgs: ['size'], bankroll: 1000 }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('MISSING_PROBS');
  });

  test('handleBasket unknown subcommand', async () => {
    installFetchMock(() => jsonResponse({}));
    const resp = await handleBasket(makeArgs({ subcommand: 'basket', positionalArgs: [] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('MISSING_SUBCOMMAND');
  });

  test('Octagon API: 502 surfaces as wrapped error', async () => {
    installFetchMock(() => new Response(JSON.stringify({ detail: 'upstream embedding failed' }), { status: 502 }));
    const resp = await handleSimilar(makeArgs({ subcommand: 'similar', query: 'foo' }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.message).toContain('502');
    expect(resp.error?.message).toContain('upstream embedding failed');
  });
});

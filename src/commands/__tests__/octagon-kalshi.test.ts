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
    activeOnly: false,
    cells: false,
    autoProbs: false,
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

  test('basket validate via --tickers (equal split)', async () => {
    let postedBody: any;
    installFetchMock(async (url, init) => {
      expect(url).toContain('/baskets/validate');
      postedBody = JSON.parse((init?.body as string) ?? '{}');
      return jsonResponse({
        total_stake_usd: 1000,
        bankroll_usd: 1000,
        max_leg_pct: 0.5,
        cluster_breakdown_thematic: { '16': ['KX-A', 'KX-B'] },
        cluster_breakdown_behavioral: {},
        unassigned_market_tickers: [],
        max_pairwise_correlation: null,
        pairwise_correlations: [],
        calendar_clashes: [],
        duplicate_underliers: [],
        warnings: ['Single leg is 50% of bankroll'],
      });
    });
    const { handleBasket } = await import('../basket.js');
    const resp = await handleBasket(makeArgs({
      subcommand: 'basket', positionalArgs: ['validate'],
      tickers: 'KX-A,KX-B', bankroll: 1000,
    }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    expect(resp.data.sub).toBe('validate');
    expect(postedBody.legs).toHaveLength(2);
    expect(postedBody.legs[0]).toEqual({ market_ticker: 'KX-A', side: 'yes', stake_usd: 500 });
  });

  test('basket size --auto-probs fetches probabilities then sizes', async () => {
    let edgeCalled = false;
    let sizeCalled = false;
    let sizeBody: any;
    installFetchMock(async (url, init) => {
      if (url.includes('/markets/edge')) {
        edgeCalled = true;
        const body = JSON.parse((init?.body as string) ?? '{}');
        expect(body.tickers).toEqual(['KX-A', 'KX-B']);
        return jsonResponse({
          run_id: 'r', captured_at: 'now', data: [
            { input_ticker: 'KX-A', market_ticker: 'KX-A', event_ticker: 'KX-A', title: null, series_category: null, model_probability: 0.62, market_probability: 0.55, edge_pp: 7, expected_return: 0.13, confidence_score: 7, total_volume: 100, total_open_interest: 50, status: 'scored', captured_at: 'now' },
            { input_ticker: 'KX-B', market_ticker: 'KX-B', event_ticker: 'KX-B', title: null, series_category: null, model_probability: null, market_probability: null, edge_pp: null, expected_return: null, confidence_score: null, total_volume: null, total_open_interest: null, status: 'unscored', captured_at: null },
          ],
        });
      }
      if (url.includes('/baskets/size')) {
        sizeCalled = true;
        sizeBody = JSON.parse((init?.body as string) ?? '{}');
        return jsonResponse({ bankroll_usd: 1000, kelly_multiplier: 0.25, total_notional: 100, legs: [] });
      }
      return jsonResponse({});
    });
    const { handleBasket } = await import('../basket.js');
    const resp = await handleBasket(makeArgs({
      subcommand: 'basket', positionalArgs: ['size'],
      autoProbs: true, tickers: 'KX-A,KX-B', bankroll: 1000, kellyMultiplier: 0.25,
    }));
    expect(resp.ok).toBe(true);
    expect(edgeCalled).toBe(true);
    expect(sizeCalled).toBe(true);
    // Only KX-A (scored) made it to the size payload
    expect(sizeBody.legs).toHaveLength(1);
    expect(sizeBody.legs[0]).toEqual({ market_ticker: 'KX-A', side: 'yes', model_probability: 0.62 });
  });

  test('basket size --auto-probs errors when nothing is scored', async () => {
    installFetchMock(async (url) => {
      if (url.includes('/markets/edge')) {
        return jsonResponse({
          run_id: 'r', captured_at: 'now', data: [
            { input_ticker: 'KX-A', market_ticker: 'KX-A', event_ticker: 'KX-A', title: null, series_category: null, model_probability: null, market_probability: null, edge_pp: null, expected_return: null, confidence_score: null, total_volume: null, total_open_interest: null, status: 'unscored', captured_at: null },
          ],
        });
      }
      return jsonResponse({});
    });
    const { handleBasket } = await import('../basket.js');
    const resp = await handleBasket(makeArgs({
      subcommand: 'basket', positionalArgs: ['size'],
      autoProbs: true, tickers: 'KX-A', bankroll: 1000,
    }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('NO_SCORED_LEGS');
  });

  test('correlate sends sides + include_cell_detail when flags set', async () => {
    let postedBody: any;
    installFetchMock(async (url, init) => {
      postedBody = JSON.parse((init?.body as string) ?? '{}');
      return jsonResponse({
        tickers: ['KX-A', 'KX-B'],
        sides: ['yes', 'no'],
        matrix: [[1, 0.5], [0.5, 1]],
        ranked_pairs: [{ ticker_a: 'KX-A', ticker_b: 'KX-B', correlation: 0.5 }],
        window_days: 30, interval: '1h', missing: [],
        cells_detail: [{ ticker_a: 'KX-A', ticker_b: 'KX-B', correlation: 0.5, overlap_count: 720, reason: 'ok' }],
      });
    });
    const { handleCorrelate } = await import('../correlate.js');
    const resp = await handleCorrelate(makeArgs({
      subcommand: 'correlate', positionalArgs: ['KX-A', 'KX-B'],
      sides: 'yes,no', cells: true, windowDays: 30,
    }));
    expect(resp.ok).toBe(true);
    expect(postedBody.sides).toEqual(['yes', 'no']);
    expect(postedBody.include_cell_detail).toBe(true);
  });

  test('correlate rejects mismatched sides length', async () => {
    installFetchMock(() => jsonResponse({}));
    const { handleCorrelate } = await import('../correlate.js');
    const resp = await handleCorrelate(makeArgs({
      subcommand: 'correlate', positionalArgs: ['KX-A', 'KX-B'],
      sides: 'yes,no,yes',
    }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('SIDES_MISMATCH');
  });

  test('series list calls /kalshi/series rollup endpoint', async () => {
    let calledUrl = '';
    installFetchMock(async (url) => {
      calledUrl = url;
      return jsonResponse({
        data: [
          { series_ticker: 'KXBTCD', series_title: 'BTC', market_count: 200, active_count: 150, total_volume_24h: 1000, dominant_category: 'Crypto', categories: ['Crypto'], last_seen_at: '2026-05-25' },
        ],
        next_cursor: null,
        has_more: false,
      });
    });
    const { handleSeries } = await import('../series.js');
    const resp = await handleSeries(makeArgs({ subcommand: 'series', positionalArgs: [] }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    if (resp.data.kind !== 'server-list') throw new Error('Expected server-list kind');
    expect(resp.data.data).toHaveLength(1);
    expect(calledUrl).toContain('/kalshi/series?');
    expect(calledUrl).toContain('sort_by=total_volume_24h');
  });

  test('basket build --tickers passes universe.market_tickers', async () => {
    let postedBody: any;
    installFetchMock(async (url, init) => {
      expect(url).toContain('/baskets/build');
      postedBody = JSON.parse((init?.body as string) ?? '{}');
      return jsonResponse({
        legs: [], realized_max_pairwise_correlation: null,
        cluster_breakdown: {}, dropped: [], universe_size: 2,
      });
    });
    const { handleBasket } = await import('../basket.js');
    const resp = await handleBasket(makeArgs({
      subcommand: 'basket', positionalArgs: ['build'],
      tickers: 'KX-A,KX-B', n: 2, maxPerCluster: 1, maxCorrelation: 0.5,
    }));
    expect(resp.ok).toBe(true);
    expect(postedBody.universe.market_tickers).toEqual(['KX-A', 'KX-B']);
    expect(postedBody.n).toBe(2);
    expect(postedBody.max_per_cluster).toBe(1);
    expect(postedBody.sizing.strategy).toBe('equal');
  });

  test('basket build --auto-probs fetches edges and switches to kelly sizing', async () => {
    let edgeCalled = false;
    let buildBody: any;
    installFetchMock(async (url, init) => {
      if (url.includes('/markets/edge')) {
        edgeCalled = true;
        const body = JSON.parse((init?.body as string) ?? '{}');
        expect(body.tickers).toEqual(['KX-A', 'KX-B']);
        return jsonResponse({
          run_id: 'r', captured_at: 'now', data: [
            { input_ticker: 'KX-A', market_ticker: 'KX-A', event_ticker: 'KX-A', title: null, series_category: null, model_probability: 0.62, market_probability: 0.55, edge_pp: 7, expected_return: 0.13, confidence_score: 7, total_volume: 100, total_open_interest: 50, status: 'scored', captured_at: 'now' },
            { input_ticker: 'KX-B', market_ticker: 'KX-B', event_ticker: 'KX-B', title: null, series_category: null, model_probability: null, market_probability: null, edge_pp: null, expected_return: null, confidence_score: null, total_volume: null, total_open_interest: null, status: 'unscored', captured_at: null },
          ],
        });
      }
      if (url.includes('/baskets/build')) {
        buildBody = JSON.parse((init?.body as string) ?? '{}');
        return jsonResponse({
          legs: [], realized_max_pairwise_correlation: null,
          cluster_breakdown: {}, dropped: [], universe_size: 2,
        });
      }
      return jsonResponse({});
    });
    const { handleBasket } = await import('../basket.js');
    const resp = await handleBasket(makeArgs({
      subcommand: 'basket', positionalArgs: ['build'],
      tickers: 'KX-A,KX-B', autoProbs: true, bankroll: 1000, kellyMultiplier: 0.25, n: 2,
    }));
    expect(resp.ok).toBe(true);
    expect(edgeCalled).toBe(true);
    expect(buildBody.universe.market_tickers).toEqual(['KX-A', 'KX-B']);
    expect(buildBody.sizing.strategy).toBe('kelly');
    // Only KX-A was scored, so leg_probabilities has 1 entry
    expect(buildBody.sizing.leg_probabilities).toEqual({ 'KX-A': 0.62 });
  });

  test('batch analyze: single POST /markets/edge for N tickers', async () => {
    let edgeBody: any;
    installFetchMock(async (url, init) => {
      expect(url).toContain('/markets/edge');
      edgeBody = JSON.parse((init?.body as string) ?? '{}');
      return jsonResponse({
        run_id: 'r1', captured_at: '2026-05-27T00:00:00Z',
        data: [
          { input_ticker: 'KX-A', market_ticker: 'KX-A', event_ticker: 'KX-A', title: 'A', series_category: 'X', model_probability: 0.6, market_probability: 0.55, edge_pp: 5, expected_return: 0.09, confidence_score: 7, total_volume: 1000, total_open_interest: 200, status: 'scored', captured_at: 'now' },
          { input_ticker: 'KX-B', market_ticker: 'KX-B', event_ticker: 'KX-B', title: null, series_category: null, model_probability: null, market_probability: null, edge_pp: null, expected_return: null, confidence_score: null, total_volume: null, total_open_interest: null, status: 'unscored', captured_at: null },
        ],
      });
    });
    const { handleAnalyzeBatch } = await import('../analyze-batch.js');
    const resp = await handleAnalyzeBatch(['KX-A', 'KX-B']);
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    expect(edgeBody.tickers).toEqual(['KX-A', 'KX-B']);
    expect(resp.data.scored).toBe(1);
    expect(resp.data.unscored).toBe(1);
  });

  test('batch analyze: rejects empty list', async () => {
    installFetchMock(() => jsonResponse({}));
    const { handleAnalyzeBatch } = await import('../analyze-batch.js');
    const resp = await handleAnalyzeBatch([]);
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('NO_TICKERS');
  });

  test('batch analyze: rejects > 100 tickers', async () => {
    installFetchMock(() => jsonResponse({}));
    const { handleAnalyzeBatch } = await import('../analyze-batch.js');
    const tickers = Array.from({ length: 101 }, (_, i) => `KX-${i}`);
    const resp = await handleAnalyzeBatch(tickers);
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('TOO_MANY_TICKERS');
  });

  test('batch analyze: dedupes + uppercases', async () => {
    let edgeBody: any;
    installFetchMock(async (_url, init) => {
      edgeBody = JSON.parse((init?.body as string) ?? '{}');
      return jsonResponse({ run_id: 'r', captured_at: 'now', data: [] });
    });
    const { handleAnalyzeBatch } = await import('../analyze-batch.js');
    await handleAnalyzeBatch(['kx-a', ' KX-A ', 'kx-b']);
    expect(edgeBody.tickers).toEqual(['KX-A', 'KX-B']);
  });

  test('series KXBTCD uses series_prefix server-side', async () => {
    let calledUrl = '';
    installFetchMock(async (url) => {
      calledUrl = url;
      return jsonResponse({
        data: [
          { market_ticker: 'KXBTCD-26DEC31-T100000', event_ticker: 'KXBTCD-26DEC31', series_ticker: null, title: 'BTC', status: 'active', close_time: null, last_price: 0.5, volume_24h: 100, category: 'Crypto' },
        ],
        next_cursor: null, has_more: false,
      });
    });
    const { handleSeries } = await import('../series.js');
    const resp = await handleSeries(makeArgs({ subcommand: 'series', positionalArgs: ['KXBTCD'] }));
    expect(resp.ok).toBe(true);
    expect(calledUrl).toContain('series_prefix=KXBTCD');
    expect(calledUrl).toContain('sort_by=volume_24h');
  });

  test('parseProbabilities rejects empty tickers and out-of-range probs', async () => {
    const { handleBasket } = await import('../basket.js');
    let sizeBody: any;
    installFetchMock(async (url, init) => {
      if (url.includes('/baskets/size')) {
        sizeBody = JSON.parse((init?.body as string) ?? '{}');
        return jsonResponse({ bankroll_usd: 1000, kelly_multiplier: 0.25, total_notional: 0, legs: [] });
      }
      return jsonResponse({});
    });
    // Inputs: " :0.5" (empty trimmed ticker), "KX-A:1.5" (>1), "KX-B:-0.1" (<0), "KX-C:0.7" (valid)
    const resp = await handleBasket(makeArgs({
      subcommand: 'basket', positionalArgs: ['size'],
      bankroll: 1000, probabilities: ' :0.5,KX-A:1.5,KX-B:-0.1,KX-C:0.7',
    }));
    expect(resp.ok).toBe(true);
    // Only KX-C survived validation
    expect(sizeBody.legs).toHaveLength(1);
    expect(sizeBody.legs[0].market_ticker).toBe('KX-C');
    expect(sizeBody.legs[0].model_probability).toBe(0.7);
  });

  test('formatMarketsWithEdgeHuman guards against invalid captured_at', async () => {
    const { formatMarketsWithEdgeHuman } = await import('../search-remote.js');
    // Should not throw when captured_at is invalid garbage.
    const out = formatMarketsWithEdgeHuman({
      run_id: '12345678-aaaa', captured_at: 'not-a-date', sort_by: 'edge_pp',
      data: [], next_cursor: null, has_more: false,
    } as any, 5);
    expect(out).toContain('captured unknown');
    // Should also handle null
    const out2 = formatMarketsWithEdgeHuman({
      run_id: '12345678-aaaa', captured_at: null, sort_by: 'edge_pp',
      data: [], next_cursor: null, has_more: false,
    } as any, 5);
    expect(out2).toContain('captured unknown');
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

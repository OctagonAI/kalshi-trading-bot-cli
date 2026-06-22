import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';
import type { ParsedArgs } from '../parse-args.js';
import { handleTrust, formatTrustHuman, type TraderTrustCard, type TrustResult } from '../trust.js';
import type { CLIResponse } from '../json.js';

function makeArgs(o: Partial<ParsedArgs>): ParsedArgs {
  return {
    subcommand: 'trust',
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

function makeCard(overrides?: Partial<TraderTrustCard>): TraderTrustCard {
  const score = (value: number) => ({
    value,
    label: value >= 70 ? 'High' : value >= 40 ? 'Moderate' : 'Low',
    drivers: [
      { name: 'depth_score', sub_score: value, points: 12.5 },
      { name: 'spread_pp', sub_score: value - 5, points: 7.3 },
      { name: 'fill_consistency', sub_score: value - 10, points: 4.2 },
    ],
    evidence: [{ metric: 'avg_spread_cents', value: 1.2 }],
    confidence: 'high' as const,
    data_freshness: 'point_in_time' as const,
  });
  return {
    calculation_version: 'trust_dashboard_v1.0',
    computed_at: '2026-06-22T15:30:00Z',
    event_ticker: 'KX-EVT',
    rollup: { median_trader_trust: 70, min_trader_trust: 55, markets_scored: 3 },
    markets: [
      {
        market_ticker: 'KX-EVT-A',
        title: 'France',
        is_primary: true,
        scores: {
          trader_trust: score(85),
          liquidity_quality: score(80),
          move_quality: score(75),
          market_avoid: score(15),
          quote_risk: score(20),
          resolution_risk: score(90),
        },
      },
      {
        market_ticker: 'KX-EVT-B',
        title: 'Brazil',
        is_primary: false,
        scores: {
          trader_trust: score(55),
          liquidity_quality: score(50),
          move_quality: score(60),
          market_avoid: score(30),
          quote_risk: score(40),
          resolution_risk: score(70),
        },
      },
    ],
    ...overrides,
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

describe('handleTrust', () => {
  let originalFetch: typeof globalThis.fetch;
  beforeEach(() => {
    process.env.OCTAGON_API_KEY = 'sk_test';
    originalFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = originalFetch;
    delete process.env.OCTAGON_API_KEY;
  });

  test('missing event ticker → error', async () => {
    installFetchMock(() => jsonResponse({}));
    const resp = await handleTrust(makeArgs({ positionalArgs: [] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('MISSING_EVENT');
  });

  test('event 404 → EVENT_NOT_FOUND', async () => {
    installFetchMock(() => new Response('{}', { status: 404 }));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('EVENT_NOT_FOUND');
  });

  test('trader_trust_json null → NO_SCORECARD (graceful, not crash)', async () => {
    installFetchMock(() => jsonResponse({
      event_ticker: 'KX-EVT', name: 'Test', trader_trust_json: null,
    }));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('NO_SCORECARD');
    expect(resp.error?.message).toMatch(/no trust scorecard/i);
  });

  test('malformed trader_trust_json → PARSE_ERROR', async () => {
    installFetchMock(() => jsonResponse({
      event_ticker: 'KX-EVT', name: 'Test', trader_trust_json: 'not json',
    }));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('PARSE_ERROR');
  });

  test('valid event returns table result', async () => {
    const card = makeCard();
    installFetchMock(() => jsonResponse({
      event_ticker: 'KX-EVT', name: 'Test event',
      trader_trust_json: JSON.stringify(card),
    }));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'] }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    if (resp.data.kind !== 'table') throw new Error();
    expect(resp.data.card.markets).toHaveLength(2);
    expect(resp.data.event_name).toBe('Test event');
  });

  test('--market drills into one market', async () => {
    const card = makeCard();
    installFetchMock(() => jsonResponse({
      event_ticker: 'KX-EVT', name: 'Test',
      trader_trust_json: JSON.stringify(card),
    }));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], market: 'KX-EVT-A' }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    if (resp.data.kind !== 'detail') throw new Error();
    expect(resp.data.market.market_ticker).toBe('KX-EVT-A');
    expect(resp.data.verbose).toBe(false);
  });

  test('--market with unknown ticker → MARKET_NOT_IN_SCORECARD', async () => {
    const card = makeCard();
    installFetchMock(() => jsonResponse({
      event_ticker: 'KX-EVT', trader_trust_json: JSON.stringify(card),
    }));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], market: 'KX-EVT-Z' }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('MARKET_NOT_IN_SCORECARD');
  });

  test('case-insensitive ticker matching for --market', async () => {
    const card = makeCard();
    installFetchMock(() => jsonResponse({
      event_ticker: 'KX-EVT', trader_trust_json: JSON.stringify(card),
    }));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['kx-evt'], market: 'kx-evt-a' }));
    expect(resp.ok).toBe(true);
  });

  test('--verbose propagates into detail result', async () => {
    const card = makeCard();
    installFetchMock(() => jsonResponse({
      event_ticker: 'KX-EVT', trader_trust_json: JSON.stringify(card),
    }));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], market: 'KX-EVT-A', verbose: true }));
    expect(resp.ok).toBe(true);
    if (!resp.ok || resp.data.kind !== 'detail') throw new Error();
    expect(resp.data.verbose).toBe(true);
  });
});

describe('formatTrustHuman', () => {
  test('table view contains rollup, header, both markets, and legend', () => {
    const card = makeCard();
    const result: TrustResult = { kind: 'table', card, event_name: 'Test event' };
    const out = formatTrustHuman(result);
    expect(out).toContain('Trader Trust scorecard for KX-EVT');
    expect(out).toContain('Test event');
    expect(out).toContain('Median trust 70');
    expect(out).toContain('trust_dashboard_v1.0');
    expect(out).toContain('KX-EVT-A');
    expect(out).toContain('KX-EVT-B');
    expect(out).toContain('France');
    expect(out).toContain('Brazil');
    // is_primary mark
    expect(out).toContain('*');
    // Legend mentions both directions of "good"
    expect(out).toMatch(/Higher is (better|worse)/i);
  });

  test('table sorted by liquidity_quality desc', () => {
    const card = makeCard();
    // Make B have higher liquidity than A
    card.markets[0].scores.liquidity_quality.value = 30;
    card.markets[1].scores.liquidity_quality.value = 90;
    const out = formatTrustHuman({ kind: 'table', card, event_name: null });
    const aIdx = out.indexOf('KX-EVT-A');
    const bIdx = out.indexOf('KX-EVT-B');
    expect(bIdx).toBeGreaterThan(0);
    expect(bIdx).toBeLessThan(aIdx);
  });

  test('detail view shows each score with label and top drivers', () => {
    const card = makeCard();
    const out = formatTrustHuman({ kind: 'detail', card, market: card.markets[0], verbose: false });
    expect(out).toContain('KX-EVT-A');
    expect(out).toContain('(primary)');
    // Each of the six score keys appears
    expect(out).toContain('Trust');
    expect(out).toContain('Liquidity');
    expect(out).toContain('Move');
    expect(out).toContain('Avoid');
    expect(out).toContain('Quote');
    expect(out).toContain('Resol');
    // Driver names present
    expect(out).toContain('depth_score');
    expect(out).toContain('spread_pp');
    expect(out).toContain('fill_consistency');
    // Risk-metric annotation on market_avoid / quote_risk
    expect(out).toContain('risk metric');
    // point_in_time → "as of report time" annotation
    expect(out).toContain('as of report time');
    // Evidence is NOT shown without --verbose
    expect(out).not.toContain('Evidence:');
  });

  test('detail view with --verbose surfaces evidence + confidence + freshness', () => {
    const card = makeCard();
    const out = formatTrustHuman({ kind: 'detail', card, market: card.markets[0], verbose: true });
    expect(out).toContain('Evidence:');
    expect(out).toContain('avg_spread_cents: 1.2');
    expect(out).toContain('Confidence: high');
    expect(out).toContain('Freshness: point_in_time');
  });
});

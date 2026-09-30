import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';
import { stripVTControlCharacters } from 'node:util';
import type { ParsedArgs } from '../parse-args.js';
import { handleTrust, formatTrustHuman, type TrustResult } from '../trust.js';
import type { TrustIndexResponse, TrustMarketScore } from '../../scan/octagon-reports-api.js';

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

/** Rendered output without ANSI codes, so assertions don't depend on the TTY. */
function render(result: TrustResult): string {
  return stripVTControlCharacters(formatTrustHuman(result));
}

function makeTrust(): TrustIndexResponse {
  const score = (value: number | null): TrustMarketScore => ({
    score: value,
    label: value === null ? null : value >= 70 ? 'High' : value >= 40 ? 'Tradeable' : 'Thin',
    confidence: 'high',
    not_applicable: value === null,
    drivers: ['24h traded notional $4,090', 'spread 1c', 'book present'],
    evidence: [{ text: 'avg spread 1.2c', window: '24h' }],
  });
  return {
    event_ticker: 'KX-EVT',
    venue: 'kalshi',
    run_id: 'run-1',
    trust_index: {
      score: 57,
      label: 'Caution',
      computed_at: '2026-06-22T15:30:00Z',
      version: 'trust_index_v2',
      caps: [],
      uncapped_score: 57,
      floors_breached: [],
      profile: {
        integrity: {
          key: 'integrity',
          name: 'Integrity',
          score: 60,
          label: 'Caution',
          risk: { score: 70, label: 'Information exposure' },
          breakdown: [
            { key: 'manipulation_resistance', name: 'Market integrity', score: 59, label: 'Caution', basis: 'measured', summary: 'Outcome is hard to influence.', factors: [] },
            { key: 'information_fairness', name: 'Info fairness', score: 40, label: 'High Risk', basis: 'structural_prior', summary: 'A small group knows first.', factors: [] },
            { key: 'settlement_reliability', name: 'Resolution quality', score: 92, label: 'Strong', basis: 'measured', summary: 'Resolution is explicit.', factors: [] },
          ],
          screen_counts: { run: 2, not_applicable: 1, awaiting_data: 1 },
        },
        trade_quality: {
          key: 'trade_quality',
          name: 'Trade quality',
          score: 46,
          label: 'High Risk',
          basis: 'measured',
          summary: 'Expect execution cost.',
          factors: ['$100 order: 4.8c slippage', '$1,000 order: book too thin to fill'],
          breakdown: [
            { key: 'liquidity', name: 'Liquidity', score: 55, label: 'Thin' },
            { key: 'move_quality', name: 'Move quality', score: 47, label: 'Mixed' },
            { key: 'rule_clarity', name: 'Rule clarity', score: 100, label: 'Clear' },
          ],
          markets: [
            {
              market_ticker: 'KX-EVT-A',
              title: 'France',
              is_primary: true,
              last_trade_cents: 53,
              scores: {
                market_quality: score(85),
                liquidity: score(80),
                move_quality: score(75),
                resolution_clarity: score(90),
              },
            },
            {
              market_ticker: 'KX-EVT-B',
              title: 'Brazil',
              is_primary: false,
              last_trade_cents: 21,
              scores: {
                market_quality: score(55),
                liquidity: score(50),
                move_quality: score(null),
                resolution_clarity: score(70),
              },
            },
          ],
          exclusions: { total: 0, terminal_lifecycle: 0, below_volume_floor: 0, no_ticker: 0 },
        },
      },
    },
  };
}

type FetchHandler = (url: string, init?: RequestInit) => Response | Promise<Response>;
let requested: string[] = [];
function installFetchMock(handler: FetchHandler): void {
  globalThis.fetch = mock(async (url: string | URL | Request, init?: RequestInit) => {
    const s = typeof url === 'string' ? url : url instanceof URL ? url.toString() : url.url;
    requested.push(s);
    return handler(s, init);
  }) as unknown as typeof fetch;
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
}

function apiError(status: number, code: string): Response {
  return jsonResponse({ error: { code, message: code } }, status);
}

describe('handleTrust', () => {
  let originalFetch: typeof globalThis.fetch;
  beforeEach(() => {
    process.env.OCTAGON_API_KEY = 'sk_test';
    originalFetch = globalThis.fetch;
    requested = [];
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

  test('reads the trust endpoint by event ticker, without expanding markets by default', async () => {
    installFetchMock(() => jsonResponse(makeTrust()));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['kx-evt'] }));
    expect(resp.ok).toBe(true);
    expect(requested).toHaveLength(1);
    expect(requested[0]).toEndWith('/predictions/reports/kalshi/KX-EVT/trust');
  });

  test('--verbose and --market expand trade_quality', async () => {
    installFetchMock(() => jsonResponse(makeTrust()));
    await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], verbose: true }));
    await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], market: 'KX-EVT-A' }));
    expect(requested).toHaveLength(2);
    for (const url of requested) expect(url).toEndWith('/trust?expand=trade_quality');
  });

  test('report_not_found → EVENT_NOT_FOUND', async () => {
    installFetchMock(() => apiError(404, 'report_not_found'));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('EVENT_NOT_FOUND');
  });

  test('trust_index_not_found → NO_SCORECARD (graceful, not crash)', async () => {
    installFetchMock(() => apiError(404, 'trust_index_not_found'));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('NO_SCORECARD');
    expect(resp.error?.message).toMatch(/no trust scorecard/i);
  });

  test('other API errors → OCTAGON_ERROR', async () => {
    installFetchMock(() => apiError(401, 'invalid_api_key'));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'] }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('OCTAGON_ERROR');
    expect(resp.error?.message).toContain('invalid_api_key');
  });

  test('valid event returns table result, verbose off by default', async () => {
    installFetchMock(() => jsonResponse(makeTrust()));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'] }));
    expect(resp.ok).toBe(true);
    if (!resp.ok || resp.data.kind !== 'table') throw new Error();
    expect(resp.data.trust.trust_index.score).toBe(57);
    expect(resp.data.verbose).toBe(false);
  });

  test('--verbose propagates into table result', async () => {
    installFetchMock(() => jsonResponse(makeTrust()));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], verbose: true }));
    expect(resp.ok).toBe(true);
    if (!resp.ok || resp.data.kind !== 'table') throw new Error();
    expect(resp.data.verbose).toBe(true);
  });

  test('expanded scorecard with no markets → EMPTY_SCORECARD', async () => {
    const trust = makeTrust();
    trust.trust_index.profile.trade_quality.markets = [];
    installFetchMock(() => jsonResponse(trust));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], verbose: true }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('EMPTY_SCORECARD');
  });

  test('--market drills into one market', async () => {
    installFetchMock(() => jsonResponse(makeTrust()));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], market: 'KX-EVT-A' }));
    expect(resp.ok).toBe(true);
    if (!resp.ok || resp.data.kind !== 'detail') throw new Error();
    expect(resp.data.market.market_ticker).toBe('KX-EVT-A');
    expect(resp.data.verbose).toBe(false);
  });

  test('--market with unknown ticker → MARKET_NOT_IN_SCORECARD', async () => {
    installFetchMock(() => jsonResponse(makeTrust()));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], market: 'KX-EVT-Z' }));
    expect(resp.ok).toBe(false);
    if (resp.ok) return;
    expect(resp.error?.code).toBe('MARKET_NOT_IN_SCORECARD');
  });

  test('case-insensitive matching for --market', async () => {
    installFetchMock(() => jsonResponse(makeTrust()));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['kx-evt'], market: 'kx-evt-a' }));
    expect(resp.ok).toBe(true);
  });

  test('--verbose propagates into detail result', async () => {
    installFetchMock(() => jsonResponse(makeTrust()));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], market: 'KX-EVT-A', verbose: true }));
    expect(resp.ok).toBe(true);
    if (!resp.ok || resp.data.kind !== 'detail') throw new Error();
    expect(resp.data.verbose).toBe(true);
  });
});

describe('formatTrustHuman — Trust Index view', () => {
  test('shows the overall score, how it adds up, and the trust profile', () => {
    const out = render({ kind: 'table', trust: makeTrust(), verbose: false });
    expect(out).toContain('Octagon Trust Index — KX-EVT');
    expect(out).toContain('Trust Index combines Integrity and Trade quality.');
    expect(out).not.toContain('Octagon Trust Index · KALSHI');
    expect(out).toContain('Integrity risk · Information exposure');
    // How it adds up — no weights: they are not in the payload
    expect(out).not.toContain('% of score');
    expect(out).toMatch(/Integrity\s+60\s+● Caution/);
    expect(out).toMatch(/Trade quality\s+46\s+● High Risk/);
    expect(out).toContain("a $1,000 order can't be filled here because the order book is too thin");
    expect(out).toMatch(/= Trust score\s+57\s+● Caution/);
    expect(out).not.toContain('before caps');
    // Trust profile
    expect(out).toContain("2 screens run · 1 don't apply · 1 awaiting data");
    expect(out).toMatch(/Market integrity\s+59\s+● Caution\s+Outcome is hard to influence\./);
    expect(out).toMatch(/Info fairness\s+40\s+● High Risk/);
    expect(out).toMatch(/Resolution quality\s+92\s+● Strong/);
    expect(out).toMatch(/Liquidity\s+55\s+● Thin/);
    expect(out).toMatch(/Move quality\s+47\s+● Mixed/);
    expect(out).toMatch(/Rule clarity\s+100\s+● Clear/);
    expect(out).toContain('Calculation trust_index_v2');
  });

  test('per-contract scores appear only with --verbose', () => {
    const plain = render({ kind: 'table', trust: makeTrust(), verbose: false });
    expect(plain).not.toContain('KX-EVT-A');
    expect(plain).toContain('trust KX-EVT --verbose');

    const verbose = render({ kind: 'table', trust: makeTrust(), verbose: true });
    expect(verbose).toContain('PER-CONTRACT MARKET QUALITY');
    expect(verbose).toMatch(/KX-EVT-A.*85/);
    expect(verbose).toMatch(/KX-EVT-B.*55/);
    expect(verbose).not.toContain('trust KX-EVT --verbose');
    expect(verbose).not.toContain('left out of the read');
  });

  test('per-contract table sorted by market quality desc', () => {
    const trust = makeTrust();
    const [france, brazil] = trust.trust_index.profile.trade_quality.markets!;
    france.scores.market_quality!.score = 30;
    brazil.scores.market_quality!.score = 90;
    const out = render({ kind: 'table', trust, verbose: true });
    expect(out.indexOf('KX-EVT-B')).toBeLessThan(out.indexOf('KX-EVT-A'));
  });

  test('excluded markets are counted with --verbose', () => {
    const trust = makeTrust();
    trust.trust_index.profile.trade_quality.exclusions = { total: 3, terminal_lifecycle: 2, below_volume_floor: 1, no_ticker: 0 };
    const out = render({ kind: 'table', trust, verbose: true });
    expect(out).toContain('3 markets left out of the read (2 closed, 1 below volume floor).');
  });

  test('caps, the uncapped score and breached floors are listed', () => {
    const trust = makeTrust();
    trust.trust_index.uncapped_score = 68;
    trust.trust_index.caps = [{ key: 'information_fairness', name: 'Information fairness', floor: 45, ceiling: 57 }];
    trust.trust_index.floors_breached = [{ key: 'settlement_reliability', floor: 50, score: 30 }];
    const out = render({ kind: 'table', trust, verbose: false });
    expect(out).toMatch(/= Trust score\s+57\s+● Caution\s+\(68 before caps\)/);
    expect(out).toContain('Caps applied: Information fairness (capped at 57)');
    expect(out).toContain('Below safety floor: Resolution quality 30 (floor 50)');
  });

  test('unscored pillars and a missing risk read as absent, not a crash', () => {
    const trust = makeTrust();
    const integrity = trust.trust_index.profile.integrity;
    integrity.risk = null;
    integrity.screen_counts = null;
    integrity.breakdown[1] = { ...integrity.breakdown[1], score: null, label: null, summary: null };
    const out = render({ kind: 'table', trust, verbose: false });
    expect(out).not.toContain('Integrity risk');
    expect(out).not.toContain('screens run');
    expect(out).toMatch(/Info fairness\s+—/);
  });
});

describe('formatTrustHuman — market detail view', () => {
  function detail(index: number, verbose: boolean): string {
    const trust = makeTrust();
    const market = trust.trust_index.profile.trade_quality.markets![index];
    return render({ kind: 'detail', trust, market, verbose });
  }

  test('a null score renders as em dash, never as zero', () => {
    const out = detail(1, false);
    expect(out).toMatch(/Move\s+—\s+\(not applicable\)/);
    expect(out).not.toMatch(/Move\s+0\/100/);
  });

  test('detail view shows each score with label and top drivers', () => {
    const out = detail(0, false);
    expect(out).toContain('KX-EVT-A');
    expect(out).toContain('(primary)');
    expect(out).toMatch(/Quality\s+85\/100\s+High/);
    expect(out).toContain('Liquidity');
    expect(out).toContain('Move');
    expect(out).toContain('Resol');
    expect(out).toContain('Last trade 53¢');
    expect(out).toContain('24h traded notional $4,090');
    // Evidence is NOT shown without --verbose
    expect(out).not.toContain('Evidence:');
  });

  test('detail view with --verbose surfaces evidence + confidence', () => {
    const out = detail(0, true);
    expect(out).toContain('Evidence:');
    expect(out).toContain('avg spread 1.2c [24h]');
    expect(out).toContain('Confidence: high');
  });

  test('fractional cents keep one decimal', () => {
    const trust = makeTrust();
    const market = trust.trust_index.profile.trade_quality.markets![1];
    market.last_trade_cents = 21.2;
    const out = render({ kind: 'detail', trust, market, verbose: false });
    expect(out).toContain('Last trade 21.2¢');
  });

  test('a score the API did not report says so', () => {
    const trust = makeTrust();
    const market = trust.trust_index.profile.trade_quality.markets![0];
    market.scores.liquidity = null;
    const out = render({ kind: 'detail', trust, market, verbose: false });
    expect(out).toMatch(/Liquidity\s+—\s+not reported/);
  });
});

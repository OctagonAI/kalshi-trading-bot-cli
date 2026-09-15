import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';
import { readFileSync } from 'node:fs';
import { stripVTControlCharacters } from 'node:util';
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

/** Rendered output without ANSI codes, so assertions don't depend on the TTY. */
function render(result: TrustResult): string {
  return stripVTControlCharacters(formatTrustHuman(result));
}

function makeCard(overrides?: Partial<TraderTrustCard>): TraderTrustCard {
  const score = (value: number | null) => ({
    value,
    label: value === null ? 'No trading in 7d' : value >= 70 ? 'Tradeable' : value >= 40 ? 'Thin' : 'Very thin',
    drivers: ['24h traded notional $4,090', 'Typical bar range 1.8% of price', 'All checks pass'],
    evidence: [
      { text: 'avg spread', metric: 'avg_spread_cents', value: 1.2, window: '24h' },
      { text: 'Light trading: under $2,000 in 24h' },
    ],
    confidence: 'high' as const,
    suppressed: false,
    not_applicable: value === null,
  });
  const screen = (value: number | null, not_applicable = false, suppressed = false) => ({
    ...score(value), not_applicable, suppressed,
  });
  return {
    calculation_version: 'trader_dashboard_lean_v1.14',
    computed_at: '2026-06-22T15:30:00Z',
    event_ticker: 'KX-EVT',
    venue: 'kalshi',
    event: {
      components: {
        event_liquidity: 55, event_liquidity_label: 'Thin',
        event_move_quality: 47, event_move_quality_label: 'Mixed',
        event_rule_clarity: 100, event_resolution: 'Clear',
      },
    },
    integrity: {
      structure: { evidence: [{ text: 'Company-reported metric: insiders know first' }] },
      scores: {
        information_exposure: screen(70),
        trade_size_anomaly: screen(43),
        outcome_control: screen(null, true),
        cross_venue_lead_lag: screen(null, false, true),
      },
    },
    underwriting: {
      calculation_version: 'underwriting_v1.3',
      profile_version: 'underwriting_profile_v0',
      underwriting_score: 57,
      underwriting_label: 'Caution',
      manipulation_resistance: { score: 59, label: 'Caution', summary: 'Outcome is hard to influence.', factors: [] },
      information_fairness: { score: 40, label: 'High Risk', summary: 'A small group knows first.', factors: [] },
      settlement_reliability: { score: 92, label: 'Strong', summary: 'Resolution is explicit.', factors: [] },
      market_quality: {
        score: 46, label: 'High Risk', summary: 'Expect execution cost.',
        factors: ['$100 order: 4.8c slippage', '$1,000 order: book too thin to fill'],
      },
      integrity_axis_score: 60,
      integrity_axis_label: 'Caution',
      caps_detail: [],
    },
    markets: [
      {
        market_ticker: 'KX-EVT-A',
        title: 'France',
        is_primary: true,
        lifecycle_status: 'active',
        fair_cents: 53,
        best_bid_cents: 52,
        best_ask_cents: 53,
        spread_cents: 1,
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
        lifecycle_status: 'active',
        fair_cents: 21.24,
        best_bid_cents: 20,
        best_ask_cents: 22,
        spread_cents: 2,
        scores: {
          market_quality: score(55),
          liquidity: score(50),
          move_quality: score(null),
          resolution_clarity: score(70),
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

  test('valid event returns table result, verbose off by default', async () => {
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
    expect(resp.data.verbose).toBe(false);
  });

  test('--verbose propagates into table result', async () => {
    const card = makeCard();
    installFetchMock(() => jsonResponse({
      event_ticker: 'KX-EVT', trader_trust_json: JSON.stringify(card),
    }));
    const resp = await handleTrust(makeArgs({ positionalArgs: ['KX-EVT'], verbose: true }));
    expect(resp.ok).toBe(true);
    if (!resp.ok || resp.data.kind !== 'table') throw new Error();
    expect(resp.data.verbose).toBe(true);
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

describe('formatTrustHuman — Trust Index view', () => {
  test('shows the overall score, how it adds up, and the trust profile', () => {
    const out = render({ kind: 'table', card: makeCard(), event_name: 'Test event', verbose: false });
    expect(out).toContain('Trust Index — KX-EVT · Test event');
    expect(out).toContain('Trust Index combines Integrity and Trade quality.');
    expect(out).toContain('Octagon Trust Index · KALSHI');
    // Headline from the integrity structure
    expect(out).toContain('Company-reported metric: insiders know first');
    expect(out).toContain('Integrity risk · Information exposure');
    // How it adds up
    expect(out).toMatch(/Integrity\s+80% of score\s+60\s+● Caution/);
    expect(out).toMatch(/Trade quality\s+20% of score\s+46\s+● High Risk/);
    expect(out).toContain("a $1,000 order can't be filled here because the order book is too thin");
    expect(out).toMatch(/= Trust score\s+57\s+● Caution/);
    // Trust profile
    expect(out).toContain("2 screens run · 1 don't apply · 1 awaiting data");
    expect(out).toMatch(/Market integrity\s+59\s+● Caution\s+Outcome is hard to influence\./);
    expect(out).toMatch(/Info fairness\s+40\s+● High Risk/);
    expect(out).toMatch(/Resolution quality\s+92\s+● Strong/);
    expect(out).toMatch(/Liquidity\s+55\s+● Thin/);
    expect(out).toMatch(/Move quality\s+47\s+● Mixed/);
    expect(out).toMatch(/Rule clarity\s+100\s+● Clear/);
  });

  test('per-contract scores appear only with --verbose', () => {
    const plain = render({ kind: 'table', card: makeCard(), event_name: null, verbose: false });
    expect(plain).not.toContain('KX-EVT-A');
    expect(plain).toContain('trust KX-EVT --verbose');

    const verbose = render({ kind: 'table', card: makeCard(), event_name: null, verbose: true });
    expect(verbose).toContain('PER-CONTRACT MARKET QUALITY');
    expect(verbose).toMatch(/KX-EVT-A.*85/);
    expect(verbose).toMatch(/KX-EVT-B.*55/);
    expect(verbose).not.toContain('trust KX-EVT --verbose');
  });

  test('per-contract table sorted by market quality desc', () => {
    const card = makeCard();
    card.markets[0].scores.market_quality.value = 30;
    card.markets[1].scores.market_quality.value = 90;
    const out = render({ kind: 'table', card, event_name: null, verbose: true });
    expect(out.indexOf('KX-EVT-B')).toBeLessThan(out.indexOf('KX-EVT-A'));
  });

  test('weights are hidden for an unknown underwriting profile', () => {
    const card = makeCard();
    card.underwriting!.profile_version = 'underwriting_profile_v9';
    const out = render({ kind: 'table', card, event_name: null, verbose: false });
    expect(out).not.toContain('% of score');
    expect(out).toMatch(/Integrity\s+60\s+● Caution/);
  });

  test('applied caps are listed', () => {
    const card = makeCard();
    card.underwriting!.caps_detail = ['information fairness below 20'];
    const out = render({ kind: 'table', card, event_name: null, verbose: false });
    expect(out).toContain('Caps applied: information fairness below 20');
  });

  test('missing underwriting block → notice, not a crash', () => {
    const out = render({ kind: 'table', card: makeCard({ underwriting: undefined }), event_name: null, verbose: false });
    expect(out).toContain('No Trust Index in the scorecard for KX-EVT yet.');
  });
});

describe('formatTrustHuman — market detail view', () => {
  test('a null score renders as em dash, never as zero', () => {
    const card = makeCard();
    const out = render({ kind: 'detail', card, market: card.markets[1], verbose: false });
    expect(out).toContain('—');
    expect(out).toContain('not applicable');
    expect(out).not.toMatch(/Move.*\b0\/100/);
  });

  test('detail view shows each score with label, quote context and top drivers', () => {
    const card = makeCard();
    const out = render({ kind: 'detail', card, market: card.markets[0], verbose: false });
    expect(out).toContain('KX-EVT-A');
    expect(out).toContain('(primary)');
    // Each of the four score keys appears
    expect(out).toContain('Quality');
    expect(out).toContain('Liquidity');
    expect(out).toContain('Move');
    expect(out).toContain('Resol');
    // Quote context from the card, in cents
    expect(out).toContain('Fair 53¢');
    expect(out).toContain('Spread 1¢');
    // Drivers are pre-rendered strings
    expect(out).toContain('24h traded notional $4,090');
    // Evidence is NOT shown without --verbose
    expect(out).not.toContain('Evidence:');
  });

  test('fractional cents keep one decimal', () => {
    const card = makeCard();
    const out = render({ kind: 'detail', card, market: card.markets[1], verbose: false });
    expect(out).toContain('Fair 21.2¢');
  });

  test('detail view with --verbose surfaces evidence + confidence', () => {
    const card = makeCard();
    const out = render({ kind: 'detail', card, market: card.markets[0], verbose: true });
    expect(out).toContain('Evidence:');
    expect(out).toContain('avg_spread_cents: 1.2 (24h)');
    // Evidence without a metric falls back to its text
    expect(out).toContain('Light trading: under $2,000 in 24h');
    expect(out).toContain('Confidence: high');
  });
});

describe('real v1.14 payload (KXNVDAA-28JANHEAD)', () => {
  const card = JSON.parse(
    readFileSync(new URL('./fixtures/trader-trust-v1.14.json', import.meta.url), 'utf8'),
  ) as TraderTrustCard;

  test('Trust Index matches the Octagon UI', () => {
    const out = render({ kind: 'table', card, event_name: null, verbose: false });
    expect(out).toMatch(/= Trust score\s+57\s+● Caution/);
    expect(out).toMatch(/Integrity\s+80% of score\s+60\s+● Caution/);
    expect(out).toMatch(/Trade quality\s+20% of score\s+46\s+● High Risk/);
    expect(out).toContain('Company-reported metric: finance and investor relations know the number before the earnings release');
    expect(out).toContain("4 screens run · 3 don't apply · 3 awaiting data");
    expect(out).toMatch(/Market integrity\s+59\s+● Caution/);
    expect(out).toMatch(/Info fairness\s+40\s+● High Risk/);
    expect(out).toMatch(/Resolution quality\s+92\s+● Strong/);
    expect(out).toMatch(/Liquidity\s+55\s+● Thin/);
    expect(out).toMatch(/Move quality\s+47\s+● Mixed/);
    expect(out).toMatch(/Rule clarity\s+100\s+● Clear/);
    expect(out).not.toContain('KXNVDAA-28JANHEAD-56000');
  });

  test('--verbose adds per-contract market quality', () => {
    const out = render({ kind: 'table', card, event_name: null, verbose: true });
    expect(out).toMatch(/KXNVDAA-28JANHEAD-48000.*77/);
    expect(out).toMatch(/KXNVDAA-28JANHEAD-56000.*14/);
  });

  test('detail view of a market with a not-applicable move score', () => {
    const market = card.markets.find((m) => m.market_ticker === 'KXNVDAA-28JANHEAD-46000')!;
    const out = render({ kind: 'detail', card, market, verbose: true });
    expect(out).toContain('No trading in 7d');
    expect(out).toContain('(not applicable)');
    expect(out).toContain('Fair 96.1¢');
    expect(out).toContain('Light recent trading');
  });
});

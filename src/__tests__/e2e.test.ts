import { describe, test, expect, beforeEach, afterEach, mock, spyOn } from 'bun:test';
import type { Database } from 'bun:sqlite';
import { tmpdir } from 'os';
import { join } from 'path';
import { readFileSync } from 'fs';

import { createDb } from '../db/index.js';
import { AuditTrail } from '../audit/trail.js';
import { ScanLoop } from '../scan/loop.js';
import { upsertTheme } from '../db/themes.js';
import { insertEdge } from '../db/edge.js';
import { insertRiskSnapshot } from '../db/risk.js';
import { openPosition } from '../db/positions.js';
import { CircuitBreaker } from '../risk/circuit-breaker.js';
import { toDollarString, fromDollarString } from '../tools/kalshi/api.js';
import { getToolRegistry } from '../tools/registry.js';
import type { OctagonVariant } from '../scan/types.js';
import type { ParsedArgs } from '../commands/parse-args.js';

// ── Helpers ──────────────────────────────────────────────────────────

function makeAudit(): { audit: AuditTrail; path: string } {
  const path = join(tmpdir(), `e2e-audit-${Date.now()}-${Math.random().toString(36).slice(2)}.jsonl`);
  return { audit: new AuditTrail(path), path };
}

function makeMockInvoker(overrides?: Partial<{
  modelProb: number;
  marketProb: number;
  mispricingSignal: string;
}>) {
  return async (_ticker: string, _variant: OctagonVariant) => {
    return JSON.stringify({
      modelProb: overrides?.modelProb ?? 0.72,
      marketProb: overrides?.marketProb ?? 0.58,
      mispricingSignal: overrides?.mispricingSignal ?? 'underpriced',
      drivers: [{ claim: 'Test driver', category: 'economic', impact: 'high' }],
      catalysts: [{ event: 'Test catalyst', date: '2026-04-01', impact: 'high', potentialMove: '+5%' }],
      sources: [{ title: 'Test source', url: 'https://example.com' }],
      resolutionHistory: 'No prior resolutions',
      contractSnapshot: 'Binary YES/NO contract',
    });
  };
}

function makeParsedArgs(overrides: Partial<ParsedArgs>): ParsedArgs {
  return {
    subcommand: 'config',
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
    snapshotLast: false,
    parseErrors: [],
    ...overrides,
  };
}

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

function setupFetchMock(originalFetch: typeof globalThis.fetch) {
  globalThis.fetch = mock(async (url: string | URL | Request) => {
    const urlStr = typeof url === 'string' ? url : url instanceof URL ? url.toString() : url.url;
    const match = urlStr.match(/\/trade-api\/v2(\/[^?]*)/);
    const path = match?.[1] ?? '';

    // Events endpoint (supports any event ticker)
    if (path.startsWith('/events/')) {
      const eventTicker = path.split('/events/')[1];
      return new Response(JSON.stringify({
        event: {
          event_ticker: eventTicker,
          markets: [{
            ticker: `${eventTicker}-MKT-YES`,
            event_ticker: eventTicker,
            status: 'open',
            last_price: 58,
            yes_bid: 55,
            yes_ask: 61,
            no_bid: 39,
            no_ask: 45,
            volume_24h: 1000,
          }],
        },
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    // Single market endpoint
    if (path.startsWith('/markets/')) {
      return new Response(JSON.stringify({
        market: {
          ticker: path.split('/markets/')[1],
          event_ticker: 'EV-1',
          status: 'open',
          last_price: 58,
          yes_bid: 55,
          yes_ask: 61,
          no_bid: 39,
          no_ask: 45,
          volume_24h: 1000,
          supports_fractional: false,
          tick_size: 1,
        },
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    // Portfolio balance
    if (path === '/portfolio/balance') {
      return new Response(JSON.stringify({
        balance: 100_000,
        payout: 20_000,
        reserved_fees: 0,
        fees: 0,
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    // Portfolio positions
    if (path === '/portfolio/positions') {
      return new Response(JSON.stringify({
        market_positions: [{ market_exposure: 20_000 }],
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    // Orderbook
    if (path.startsWith('/orderbook')) {
      return new Response(JSON.stringify({
        orderbook: { yes: [[55, 100], [54, 200]], no: [[45, 100], [44, 200]] },
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    // Orders
    if (path === '/portfolio/orders') {
      return new Response(JSON.stringify({
        order: { order_id: 'ORD-TEST-1', status: 'resting', ticker: 'MKT-YES' },
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    return new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } });
  }) as unknown as typeof fetch;
}

// ── Test Suite ────────────────────────────────────────────────────────

describe('E2E Integration Tests', () => {
  let db: Database;
  let audit: AuditTrail;
  let auditPath: string;
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    db = createDb(':memory:');
    const a = makeAudit();
    audit = a.audit;
    auditPath = a.path;

    process.env.KALSHI_API_KEY = 'test-key';
    process.env.KALSHI_PRIVATE_KEY = TEST_PRIVATE_KEY;

    originalFetch = globalThis.fetch;
    setupFetchMock(originalFetch);
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    delete process.env.KALSHI_API_KEY;
    delete process.env.KALSHI_PRIVATE_KEY;
  });

  // Test 1: scan --theme runs full cycle
  test('scan --theme runs full cycle with edge snapshots', async () => {
    upsertTheme(db, { theme_id: 'my-crypto', name: 'My Crypto', tickers: '["EV-CRYPTO"]' });
    const loop = new ScanLoop(db, audit, makeMockInvoker());

    const result = await loop.runOnce({ theme: 'my-crypto' });

    expect(result.edgeSnapshots.length).toBeGreaterThanOrEqual(1);
    expect(result.eventsScanned).toBe(1);

    const edgeRows = db.query('SELECT * FROM edge_history').all();
    expect(edgeRows.length).toBeGreaterThanOrEqual(1);

    loop.stop();
  });

  // Test 2: watch starts and stops cleanly
  test('watch starts and stops cleanly', async () => {
    upsertTheme(db, { theme_id: 'watch-test', name: 'Watch', tickers: '["EV-W"]' });
    const loop = new ScanLoop(db, audit, makeMockInvoker());

    loop.start({ theme: 'watch-test', intervalMinutes: 15 });

    // Wait for first cycle to complete
    await new Promise((r) => setTimeout(r, 2000));

    // Verify audit has SCAN_COMPLETE
    const lines = readFileSync(auditPath, 'utf-8').trim().split('\n').map((l) => JSON.parse(l));
    const types = lines.map((l: { type: string }) => l.type);
    expect(types).toContain('SCAN_COMPLETE');

    // Stop cleanly — no error expected
    loop.stop();
  });

  // Test 3: edge --json returns structured data
  test('edge --json returns structured edge data', async () => {
    // Pre-seed edge_history
    insertEdge(db, {
      ticker: 'MKT-YES',
      event_ticker: 'EV-1',
      timestamp: Math.floor(Date.now() / 1000),
      model_prob: 0.72,
      market_prob: 0.58,
      edge: 0.14,
      confidence: 'high',
      drivers_json: JSON.stringify([{ claim: 'Strong signal', category: 'economic', impact: 'high' }]),
    });

    // Mock getDb to return our in-memory DB
    const dbModule = await import('../db/index.js');
    const dbSpy = spyOn(dbModule, 'getDb').mockReturnValue(db);

    const { handleEdge } = await import('../commands/edge.js');
    const resp = await handleEdge(makeParsedArgs({ subcommand: 'edge', json: true }));

    expect(resp.ok).toBe(true);
    expect(resp.command).toBe('edge');
    expect(Array.isArray(resp.data)).toBe(true);
    expect(resp.data.length).toBeGreaterThanOrEqual(1);

    dbSpy.mockRestore();
  });

  // Test 4: edge filters out records older than 24h
  test('edge excludes records older than 24h by default', async () => {
    const now = Math.floor(Date.now() / 1000);
    // Recent edge (1 hour ago)
    insertEdge(db, {
      ticker: 'RECENT-YES',
      event_ticker: 'EV-R',
      timestamp: now - 3600,
      model_prob: 0.70,
      market_prob: 0.60,
      edge: 0.10,
      confidence: 'high',
    });
    // Old edge (48 hours ago)
    insertEdge(db, {
      ticker: 'OLD-YES',
      event_ticker: 'EV-O',
      timestamp: now - 48 * 3600,
      model_prob: 0.65,
      market_prob: 0.55,
      edge: 0.10,
      confidence: 'high',
    });

    const dbModule = await import('../db/index.js');
    const dbSpy = spyOn(dbModule, 'getDb').mockReturnValue(db);

    const { handleEdge } = await import('../commands/edge.js');
    const resp = await handleEdge(makeParsedArgs({ subcommand: 'edge' }));

    expect(resp.ok).toBe(true);
    const tickers = resp.data.map((r) => r.ticker);
    expect(tickers).toContain('RECENT-YES');
    expect(tickers).not.toContain('OLD-YES');

    dbSpy.mockRestore();
  });

  // Test 5: portfolio shows positions with edge data
  test('portfolio shows positions with edge data', async () => {
    // Seed open position
    openPosition(db, {
      position_id: 'pos-1',
      ticker: 'MKT-YES',
      event_ticker: 'MKT',
      direction: 'yes',
      size: 5,
      entry_price: 58,
      entry_edge: 0.14,
      entry_kelly: 0.05,
      current_pnl: 100,
      status: 'open',
      opened_at: Math.floor(Date.now() / 1000) - 3600,
    });

    // Seed edge data for that ticker
    insertEdge(db, {
      ticker: 'MKT-YES',
      event_ticker: 'MKT',
      timestamp: Math.floor(Date.now() / 1000),
      model_prob: 0.70,
      market_prob: 0.60,
      edge: 0.10,
      confidence: 'high',
    });

    const dbModule = await import('../db/index.js');
    const dbSpy = spyOn(dbModule, 'getDb').mockReturnValue(db);

    const { handlePortfolio } = await import('../commands/portfolio.js');
    const resp = await handlePortfolio(makeParsedArgs({ subcommand: 'portfolio' }));

    expect(resp.ok).toBe(true);
    expect(resp.data.positions.length).toBeGreaterThan(0);
    expect(resp.data.positions[0].entryEdge).toBe(0.14);
    expect(resp.data.positions[0].currentEdge).toBe(0.10);

    dbSpy.mockRestore();
  });

  // Test 6: audit.jsonl contains event types from scan cycle
  test('audit trail contains expected event types from scan cycle', async () => {
    upsertTheme(db, { theme_id: 'audit-test', name: 'Audit', tickers: '["EV-AUDIT"]' });
    const loop = new ScanLoop(db, audit, makeMockInvoker());

    await loop.runOnce({ theme: 'audit-test' });

    const lines = readFileSync(auditPath, 'utf-8')
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l));

    const types = lines.map((l: { type: string }) => l.type);
    expect(types).toContain('SCAN_COMPLETE');
    expect(types).toContain('OCTAGON_CALL');
    expect(types).toContain('EDGE_DETECTED');

    loop.stop();
  });

  // Test 7: Kalshi client handles fixed-point prices
  test('Kalshi client handles fixed-point price conversions', () => {
    // Round-trip test
    expect(fromDollarString(toDollarString(58))).toBe(58);

    // Edge cases
    expect(toDollarString(0)).toBe('0.00');
    expect(toDollarString(99)).toBe('0.99');
    expect(toDollarString(100)).toBe('1.00');

    // Additional round-trips
    expect(fromDollarString(toDollarString(1))).toBe(1);
    expect(fromDollarString(toDollarString(50))).toBe(50);
    expect(fromDollarString('0.55')).toBe(55);
  });

  // Test 8: Circuit breaker activates on drawdown breach
  test('circuit breaker activates on drawdown breach', () => {
    insertRiskSnapshot(db, {
      timestamp: Math.floor(Date.now() / 1000),
      cash_balance: 80_000,
      portfolio_value: 80_000,
      drawdown_current: 0.25,
      drawdown_max: 0.25,
    });

    const cb = new CircuitBreaker({ maxDrawdown: 0.20 });
    const status = cb.check(db);

    expect(status.active).toBe(true);
    expect(status.reason).toContain('Drawdown');
  });

  // Test 9: Brier score in portfolio --performance
  test('portfolio --performance includes Brier scores', async () => {
    // Seed closed positions
    openPosition(db, {
      position_id: 'perf-1',
      ticker: 'MKT-YES',
      event_ticker: 'EV-ECON',
      direction: 'yes',
      size: 5,
      entry_price: 58,
      entry_edge: 0.14,
      current_pnl: 500,
      status: 'closed',
      opened_at: Math.floor(Date.now() / 1000) - 86400,
      closed_at: Math.floor(Date.now() / 1000) - 3600,
    });

    // Seed edge_history
    insertEdge(db, {
      ticker: 'MKT-YES',
      event_ticker: 'EV-ECON',
      timestamp: Math.floor(Date.now() / 1000) - 7200,
      model_prob: 0.72,
      market_prob: 0.58,
      edge: 0.14,
      confidence: 'high',
    });

    // Seed brier_scores
    db.prepare(`
      INSERT INTO brier_scores (ticker, event_ticker, category, model_prob, actual_outcome, brier_score, settled_at)
      VALUES ($ticker, $event_ticker, $category, $model_prob, $actual_outcome, $brier_score, $settled_at)
    `).run({
      $ticker: 'MKT-YES',
      $event_ticker: 'EV-ECON',
      $category: 'EV',
      $model_prob: 0.72,
      $actual_outcome: 1,
      $brier_score: 0.078,
      $settled_at: Math.floor(Date.now() / 1000) - 1800,
    });

    // Seed risk snapshots for Sharpe
    insertRiskSnapshot(db, {
      timestamp: Math.floor(Date.now() / 1000) - 86400,
      cash_balance: 95_000,
      portfolio_value: 95_000,
      daily_pnl: -200,
    });
    insertRiskSnapshot(db, {
      timestamp: Math.floor(Date.now() / 1000),
      cash_balance: 100_000,
      portfolio_value: 100_000,
      daily_pnl: 500,
    });

    const dbModule = await import('../db/index.js');
    const dbSpy = spyOn(dbModule, 'getDb').mockReturnValue(db);

    const { handlePortfolio } = await import('../commands/portfolio.js');
    const resp = await handlePortfolio(makeParsedArgs({ subcommand: 'portfolio', performance: true }));

    expect(resp.ok).toBe(true);
    expect(resp.data.performance).toBeDefined();
    expect(typeof resp.data.performance!.totalPnl).toBe('number');

    const brierEntries = Object.entries(resp.data.performance!.brierByCategory);
    expect(brierEntries.length).toBeGreaterThan(0);

    dbSpy.mockRestore();
  });

  // Test 10: Chat mode tool registry loads
  test('chat mode tool registry loads expected tools', () => {
    const tools = getToolRegistry('gpt-4o');
    const names = tools.map((t) => t.name);

    expect(names).toContain('kalshi_search');
    expect(names).toContain('kalshi_trade');
    expect(names).toContain('portfolio_overview');
    expect(names).toContain('exchange_status');
    expect(names).toContain('web_fetch');
  });
});

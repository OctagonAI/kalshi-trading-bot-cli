import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';
import type { Database } from 'bun:sqlite';
import { tmpdir } from 'os';
import { join } from 'path';
import { readFileSync } from 'fs';

import { createDb } from '../../db/index.js';
import { AuditTrail } from '../../audit/trail.js';
import { ScanLoop } from '../loop.js';
import { upsertTheme } from '../../db/themes.js';
import { getLatestSnapshot } from '../../db/risk.js';
import type { OctagonVariant } from '../types.js';

function makeAudit(): { audit: AuditTrail; path: string } {
  const path = join(tmpdir(), `test-audit-${Date.now()}-${Math.random().toString(36).slice(2)}.jsonl`);
  return { audit: new AuditTrail(path), path };
}

function makeMockInvoker() {
  return async (_ticker: string, _variant: OctagonVariant) => {
    return JSON.stringify({
      modelProb: 72,
      marketProb: 58,
      mispricingSignal: 'underpriced',
      drivers: [{ claim: 'Test driver', category: 'economic', impact: 'high' }],
      catalysts: [],
      sources: [],
      resolutionHistory: '',
      contractSnapshot: '',
    });
  };
}

describe('ScanLoop', () => {
  let db: Database;
  let audit: AuditTrail;
  let auditPath: string;
  let loop: ScanLoop;
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    db = createDb(':memory:');
    const a = makeAudit();
    audit = a.audit;
    auditPath = a.path;

    // Seed theme with one event ticker
    upsertTheme(db, { theme_id: 'test-theme', name: 'Test', tickers: '["EV-1"]' });

    // Mock fetch for all Kalshi API calls
    originalFetch = globalThis.fetch;
    globalThis.fetch = mock(async (url: string | URL | Request) => {
      const urlStr = typeof url === 'string' ? url : url instanceof URL ? url.toString() : url.url;

      // Extract path from full URL
      const match = urlStr.match(/\/trade-api\/v2(\/[^?]*)/);
      const path = match?.[1] ?? '';

      // Events endpoint
      if (path === '/events/EV-1' || urlStr.includes('/events/EV-1')) {
        return new Response(JSON.stringify({
          event: {
            event_ticker: 'EV-1',
            markets: [{
              ticker: 'MKT-YES',
              event_ticker: 'EV-1',
              status: 'open',
              last_price: 58,
              yes_bid: 55,
              yes_ask: 61,
              volume_24h: 1000,
            }],
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

      return new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } });
    }) as unknown as typeof fetch;

    loop = new ScanLoop(db, audit, makeMockInvoker());
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    loop.stop();
  });

  test('runs one full scan cycle', async () => {
    const result = await loop.runOnce({ theme: 'test-theme' });

    expect(result.scanId).toBeTruthy();
    expect(result.eventsScanned).toBe(1);
    expect(result.edgeSnapshots.length).toBe(1);
    expect(result.duration).toBeGreaterThanOrEqual(0);
  });

  test('inserts edge_history rows', async () => {
    await loop.runOnce({ theme: 'test-theme' });

    const rows = db.query('SELECT * FROM edge_history').all() as Array<{ ticker: string }>;
    expect(rows.length).toBeGreaterThanOrEqual(1);
    expect(rows[0].ticker).toBe('MKT-YES');
  });

  test('creates risk_snapshots with bankroll data', async () => {
    await loop.runOnce({ theme: 'test-theme' });

    const snapshot = getLatestSnapshot(db);
    expect(snapshot).not.toBeNull();
    expect(snapshot!.cash_balance).toBe(100_000);
  });

  test('audit trail has SCAN_START and SCAN_COMPLETE', async () => {
    await loop.runOnce({ theme: 'test-theme' });

    const lines = readFileSync(auditPath, 'utf-8')
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l));

    const types = lines.map((l: { type: string }) => l.type);
    expect(types).toContain('SCAN_START');
    expect(types).toContain('SCAN_COMPLETE');
  });

  test('emits alerts for high-confidence edges', async () => {
    const result = await loop.runOnce({ theme: 'test-theme' });

    // Edge of ~0.14 = very_high confidence → should produce EDGE_DETECTED alert
    const edgeAlerts = result.alerts.filter((a) => a.alertType === 'EDGE_DETECTED');
    expect(edgeAlerts.length).toBeGreaterThanOrEqual(1);

    // Alert should be persisted to DB
    const dbAlerts = db.query('SELECT * FROM alerts').all();
    expect(dbAlerts.length).toBeGreaterThanOrEqual(1);
  });

  test('dryRun computes but skips alert persistence', async () => {
    const result = await loop.runOnce({ theme: 'test-theme', dryRun: true });

    // Alerts should still be collected in result
    expect(result.alerts.length).toBeGreaterThanOrEqual(1);

    // But NOT persisted to the alerts table
    const dbAlerts = db.query('SELECT * FROM alerts').all();
    expect(dbAlerts.length).toBe(0);
  });
});

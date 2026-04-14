import { describe, test, expect, beforeEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { createDb } from '../../db/index.js';
import { AuditTrail } from '../../audit/trail.js';
import { OctagonClient } from '../octagon-client.js';
import { getLatestReport, getTtlForCloseTime } from '../../db/octagon-cache.js';
import { insertEdge, getLatestEdge } from '../../db/edge.js';
import type { OctagonInvoker, OctagonVariant } from '../types.js';
import { tmpdir } from 'os';
import { join } from 'path';

// --- Fixtures ---

const REALISTIC_JSON_RESPONSE = JSON.stringify({
  modelProb: 72,
  marketProb: 65,
  mispricingSignal: 'underpriced',
  drivers: [
    { claim: 'Strong polling momentum', category: 'political', impact: 'high', sourceUrl: 'https://example.com/poll' },
    { claim: 'Economic headwinds', category: 'economic', impact: 'medium' },
  ],
  catalysts: [
    { date: '2026-03-25', event: 'Primary election results', impact: 'high', potentialMove: '+/- 10%' },
    { date: '2026-04-01', event: 'Jobs report', impact: 'medium', potentialMove: '+/- 3%' },
  ],
  sources: [
    { url: 'https://example.com/poll', title: 'Latest Polling Data' },
    { url: 'https://example.com/economic', title: 'Economic Forecast' },
  ],
  resolutionHistory: 'Market created 2026-01-15. No prior resolution.',
  contractSnapshot: 'Yes contract at $0.65, volume 12,000',
});

const REALISTIC_MARKDOWN_RESPONSE = `
# Octagon Analysis: TICKER-YES

## Model Probability
Model probability: 72%

## Market Price
Market price: 65%

## Mispricing Assessment
Mispricing signal: underpriced

## Price Drivers
- **Strong polling momentum** — latest polls show 5pt lead
- **Economic headwinds** — GDP growth slowing

## Catalysts
- **2026-03-25** — Primary election results (high impact)
- **2026-04-01** — Jobs report release

## Sources
[Latest Polling Data](https://example.com/poll)
[Economic Forecast](https://example.com/economic)
https://example.com/raw-source

## Resolution History
Market created 2026-01-15. No prior resolution.

## Contract Snapshot
Yes contract at $0.65, volume 12,000
`;

const MINIMAL_RESPONSE = 'No data available for this ticker.';

// --- Helpers ---

function makeInvoker(response: string): OctagonInvoker {
  return async (_ticker: string, _variant: OctagonVariant) => response;
}

function makeTrackingInvoker(response: string): {
  invoker: OctagonInvoker;
  calls: Array<{ ticker: string; variant: OctagonVariant }>;
} {
  const calls: Array<{ ticker: string; variant: OctagonVariant }> = [];
  const invoker: OctagonInvoker = async (ticker, variant) => {
    calls.push({ ticker, variant });
    return response;
  };
  return { invoker, calls };
}

function makeAudit(): AuditTrail {
  return new AuditTrail(join(tmpdir(), `test-audit-${Date.now()}-${Math.random().toString(36).slice(2)}.jsonl`));
}

// --- Tests ---

describe('OctagonClient', () => {
  let db: Database;
  let audit: AuditTrail;

  beforeEach(() => {
    db = createDb(':memory:');
    audit = makeAudit();
  });

  describe('parseReport', () => {
    test('parses JSON response correctly', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const report = client.parseReport(REALISTIC_JSON_RESPONSE, 'TICKER-YES', 'EVENT-1', 'default');

      expect(report.ticker).toBe('TICKER-YES');
      expect(report.eventTicker).toBe('EVENT-1');
      expect(report.modelProb).toBe(0.72);
      expect(report.marketProb).toBe(0.65);
      expect(report.mispricingSignal).toBe('underpriced');
      expect(report.variantUsed).toBe('default');

      expect(report.drivers).toHaveLength(2);
      expect(report.drivers[0].claim).toBe('Strong polling momentum');
      expect(report.drivers[0].category).toBe('political');
      expect(report.drivers[0].impact).toBe('high');
      expect(report.drivers[0].sourceUrl).toBe('https://example.com/poll');
      expect(report.drivers[1].category).toBe('economic');

      expect(report.catalysts).toHaveLength(2);
      expect(report.catalysts[0].date).toBe('2026-03-25');
      expect(report.catalysts[0].event).toBe('Primary election results');
      expect(report.catalysts[0].impact).toBe('high');
      expect(report.catalysts[0].potentialMove).toBe('+/- 10%');

      expect(report.sources).toHaveLength(2);
      expect(report.sources[0].url).toBe('https://example.com/poll');
      expect(report.sources[0].title).toBe('Latest Polling Data');

      expect(report.resolutionHistory).toBe('Market created 2026-01-15. No prior resolution.');
      expect(report.contractSnapshot).toBe('Yes contract at $0.65, volume 12,000');
    });

    test('parses markdown response correctly', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const report = client.parseReport(REALISTIC_MARKDOWN_RESPONSE, 'TICKER-YES', 'EVENT-1', 'cache');

      expect(report.modelProb).toBe(0.72);
      expect(report.marketProb).toBe(0.65);
      expect(report.mispricingSignal).toBe('underpriced');
      expect(report.variantUsed).toBe('cache');

      expect(report.drivers.length).toBeGreaterThanOrEqual(2);
      expect(report.drivers[0].claim).toContain('Strong polling momentum');

      expect(report.catalysts.length).toBeGreaterThanOrEqual(1);
      expect(report.catalysts[0].date).toBe('2026-03-25');

      expect(report.sources.length).toBeGreaterThanOrEqual(2);

      expect(report.resolutionHistory).toContain('Market created');
      expect(report.contractSnapshot).toContain('$0.65');
    });

    test('returns sensible defaults for minimal/malformed input', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const report = client.parseReport(MINIMAL_RESPONSE, 'BAD-TICKER', 'EVENT-X', 'default');

      expect(report.ticker).toBe('BAD-TICKER');
      expect(report.eventTicker).toBe('EVENT-X');
      expect(report.modelProb).toBe(0.5);
      expect(report.marketProb).toBe(0.5);
      expect(report.mispricingSignal).toBe('fair_value');
      expect(report.drivers).toEqual([]);
      expect(report.catalysts).toEqual([]);
      expect(report.sources).toEqual([]);
      expect(report.resolutionHistory).toBe('');
      expect(report.contractSnapshot).toBe('');
    });

    test('handles sub-1% numeric values (e.g. 0.886 meaning 0.886%)', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const json = JSON.stringify({ modelProb: 0.886, marketProb: 65 });
      const report = client.parseReport(json, 'T', 'E', 'default');
      expect(report.modelProb).toBeCloseTo(0.00886, 5);
      expect(report.marketProb).toBe(0.65);
    });

    test('extracts per-market probability from outcome_probabilities_json', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const json = JSON.stringify({
        versions: [{
          model_probability: 1.6,
          market_probability: 1.3,
          outcome_probabilities_json: JSON.stringify([
            { market_ticker: 'KXPRESNOMR-28-TMAS', model_probability: 1.6, market_probability: 1.3 },
            { market_ticker: 'KXPRESNOMR-28-JDV', model_probability: 37.69, market_probability: 37.0 },
            { market_ticker: 'KXPRESNOMR-28-MR', model_probability: 28.77, market_probability: 25.0 },
          ]),
        }],
      });

      const report = client.parseReport(json, 'KXPRESNOMR-28-JDV', 'KXPRESNOMR-28', 'cache');
      expect(report.modelProb).toBeCloseTo(0.3769, 4);  // 37.69%, not 1.6%
      expect(report.marketProb).toBeCloseTo(0.37, 4);
    });

    test('handles outcome_probabilities_json as array (not string) with case-insensitive ticker match', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const json = JSON.stringify({
        versions: [{
          model_probability: 1.6,
          market_probability: 1.3,
          outcome_probabilities_json: [
            { market_ticker: 'KXPRESNOMR-28-TMAS', model_probability: 1.6, market_probability: 1.3 },
            { market_ticker: 'KXPRESNOMR-28-JDV', model_probability: 37.69, market_probability: 37.0 },
          ],
        }],
      });

      // Mixed-case ticker should still match
      const report = client.parseReport(json, 'kxpresnomr-28-jdv', 'KXPRESNOMR-28', 'cache');
      expect(report.modelProb).toBeCloseTo(0.3769, 4);
      expect(report.marketProb).toBeCloseTo(0.37, 4);
    });

    test('falls back to event-level probability when ticker not in outcome_probabilities_json', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const json = JSON.stringify({
        versions: [{
          model_probability: 55.0,
          market_probability: 50.0,
          outcome_probabilities_json: JSON.stringify([
            { market_ticker: 'OTHER-TICKER', model_probability: 30.0 },
          ]),
        }],
      });

      const report = client.parseReport(json, 'MISSING-TICKER', 'EVENT-1', 'cache');
      expect(report.modelProb).toBeCloseTo(0.55, 4);  // falls back to event-level
      expect(report.marketProb).toBeCloseTo(0.50, 4);
    });

    test('handles percentage values (e.g. "72%")', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const json = JSON.stringify({ modelProb: '72%', marketProb: '65%' });
      const report = client.parseReport(json, 'T', 'E', 'default');
      expect(report.modelProb).toBe(0.72);
      expect(report.marketProb).toBe(0.65);
    });
  });

  describe('shouldRefresh', () => {
    test('(a) triggers refresh when price moves >5%', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);

      insertEdge(db, {
        ticker: 'MOVE-YES',
        event_ticker: 'EV-1',
        timestamp: Math.floor(Date.now() / 1000),
        model_prob: 0.60,
        market_prob: 0.50,
        edge: 0.10,
      });

      const result = client.shouldRefresh('MOVE-YES', 0.56);
      expect(result.refresh).toBe(true);
      expect(result.reason).toContain('price moved');
    });

    test('(a) does NOT trigger for small price changes', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);

      insertEdge(db, {
        ticker: 'STABLE-YES',
        event_ticker: 'EV-1',
        timestamp: Math.floor(Date.now() / 1000),
        model_prob: 0.60,
        market_prob: 0.50,
        edge: 0.10,
      });

      const result = client.shouldRefresh('STABLE-YES', 0.52);
      expect(result.refresh).toBe(false);
    });

    test('(b) triggers refresh when edge flips sign', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);

      // Positive edge: model(0.60) > market(0.50) → edge = +0.10
      insertEdge(db, {
        ticker: 'FLIP-YES',
        event_ticker: 'EV-1',
        timestamp: Math.floor(Date.now() / 1000),
        model_prob: 0.60,
        market_prob: 0.50,
        edge: 0.10,
      });

      // Now market is at 0.65 → implied edge = 0.60 - 0.65 = -0.05 (sign flipped)
      const result = client.shouldRefresh('FLIP-YES', 0.65);
      // Price moved 15% so trigger (a) fires first, but both would trigger
      expect(result.refresh).toBe(true);
    });

    test('(b) edge flip with small price change', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);

      // Small positive edge
      insertEdge(db, {
        ticker: 'FLIP2-YES',
        event_ticker: 'EV-1',
        timestamp: Math.floor(Date.now() / 1000),
        model_prob: 0.52,
        market_prob: 0.50,
        edge: 0.02,
      });

      // market at 0.54 → implied edge = 0.52 - 0.54 = -0.02 (flipped)
      // price delta = 0.04 < 0.05 threshold, so (a) won't trigger, but (b) will
      const result = client.shouldRefresh('FLIP2-YES', 0.54);
      expect(result.refresh).toBe(true);
      expect(result.reason).toContain('edge flipped');
    });

    test('(c) triggers refresh when high-impact catalyst date reached', async () => {
      const client = new OctagonClient(makeInvoker(REALISTIC_JSON_RESPONSE), db, audit);

      // Insert a report with a high-impact catalyst dated today
      const today = new Date().toISOString().slice(0, 10);
      const catalysts = [{ date: today, event: 'Test event', impact: 'high', potentialMove: '+5%' }];

      const { insertReport: insert } = await import('../../db/octagon-cache.js');
      insert(db, {
        report_id: 'test-report-1',
        ticker: 'CAT-YES',
        event_ticker: 'EV-1',
        model_prob: 0.60,
        market_prob: 0.55,
        fetched_at: Math.floor(Date.now() / 1000) - 3600,
        expires_at: Math.floor(Date.now() / 1000) + 83000,
        catalysts_json: JSON.stringify(catalysts),
      });

      const result = client.shouldRefresh('CAT-YES', 0.55);
      expect(result.refresh).toBe(true);
      expect(result.reason).toContain('catalyst');
    });

    test('(d) triggers refresh for manual request', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const result = client.shouldRefresh('ANY-YES', 0.50, true);
      expect(result.refresh).toBe(true);
      expect(result.reason).toContain('manual');
    });

    test('negative: no triggers met', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);

      insertEdge(db, {
        ticker: 'QUIET-YES',
        event_ticker: 'EV-1',
        timestamp: Math.floor(Date.now() / 1000),
        model_prob: 0.60,
        market_prob: 0.50,
        edge: 0.10,
      });

      // Small move, same sign edge
      const result = client.shouldRefresh('QUIET-YES', 0.52);
      expect(result.refresh).toBe(false);
      expect(result.reason).toContain('no refresh triggers');
    });
  });

  describe('credit ceiling', () => {
    test('default variant uses cache (no credits)', async () => {
      const { invoker, calls } = makeTrackingInvoker(REALISTIC_JSON_RESPONSE);
      const client = new OctagonClient(invoker, db, audit, { dailyCreditCeiling: 6 });

      // Default variant maps to cache — costs 0 credits
      await client.fetchReport('T1-YES', 'EV-1', 'default');
      expect(client.getCreditsUsed()).toBe(0);
      expect(calls[0].variant).toBe('cache');
    });

    test('auto-downgrades refresh to cache when ceiling exhausted', async () => {
      const { invoker, calls } = makeTrackingInvoker(REALISTIC_JSON_RESPONSE);
      const client = new OctagonClient(invoker, db, audit, { dailyCreditCeiling: 6 });

      // First refresh: 3 credits
      await client.fetchReport('T1-YES', 'EV-1', 'refresh');
      expect(client.getCreditsUsed()).toBe(3);

      // Second refresh: 6 credits
      await client.fetchReport('T2-YES', 'EV-1', 'refresh');
      expect(client.getCreditsUsed()).toBe(6);

      // Third refresh: would be 9, auto-downgrades to cache
      await client.fetchReport('T3-YES', 'EV-1', 'refresh');
      expect(client.getCreditsUsed()).toBe(6); // stays at 6

      expect(calls[0].variant).toBe('refresh');
      expect(calls[1].variant).toBe('refresh');
      expect(calls[2].variant).toBe('cache');
    });

    test('resetCredits resets the counter', async () => {
      const client = new OctagonClient(makeInvoker(REALISTIC_JSON_RESPONSE), db, audit, { dailyCreditCeiling: 6 });

      await client.fetchReport('T1-YES', 'EV-1', 'refresh');
      expect(client.getCreditsUsed()).toBe(3);

      client.resetCredits();
      expect(client.getCreditsUsed()).toBe(0);
    });

    test('cache variant never costs credits', async () => {
      const client = new OctagonClient(makeInvoker(REALISTIC_JSON_RESPONSE), db, audit);

      await client.fetchReport('T1-YES', 'EV-1', 'cache');
      await client.fetchReport('T2-YES', 'EV-1', 'cache');
      expect(client.getCreditsUsed()).toBe(0);
    });
  });

  describe('DB storage', () => {
    test('fetchReport persists correct row to database', async () => {
      const client = new OctagonClient(makeInvoker(REALISTIC_JSON_RESPONSE), db, audit);

      const report = await client.fetchReport('DB-YES', 'EV-DB', 'refresh');

      const row = getLatestReport(db, 'DB-YES');
      expect(row).not.toBeNull();
      expect(row!.ticker).toBe('DB-YES');
      expect(row!.event_ticker).toBe('EV-DB');
      expect(row!.model_prob).toBe(0.72);
      expect(row!.market_prob).toBe(0.65);
      expect(row!.mispricing_signal).toBe('underpriced');
      expect(row!.variant_used).toBe('refresh');

      // drivers_json is parseable
      const drivers = JSON.parse(row!.drivers_json!);
      expect(drivers).toHaveLength(2);
      expect(drivers[0].claim).toBe('Strong polling momentum');

      // expires_at = fetchedAt + 86400
      expect(row!.expires_at).toBe(row!.fetched_at + 86400);
    });

    test('toDbRow generates valid UUID and sets expires_at', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const report = client.parseReport(REALISTIC_JSON_RESPONSE, 'T', 'E', 'default');
      const row = client.toDbRow(report);

      // UUID format check
      expect(row.report_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
      expect(row.expires_at).toBe(row.fetched_at + 86400);
      expect(row.ticker).toBe('T');
      expect(row.event_ticker).toBe('E');
    });

    test('toDbRow uses tiered TTL when closeTimeEpoch provided', () => {
      const client = new OctagonClient(makeInvoker(''), db, audit);
      const report = client.parseReport(REALISTIC_JSON_RESPONSE, 'T', 'E', 'default');

      // Close in 12 hours → 1h TTL
      const closeIn12h = report.fetchedAt + 12 * 3600;
      const row12h = client.toDbRow(report, closeIn12h);
      expect(row12h.expires_at).toBe(row12h.fetched_at + 3600);

      // Close in 3 days → 6h TTL
      const closeIn3d = report.fetchedAt + 3 * 86400;
      const row3d = client.toDbRow(report, closeIn3d);
      expect(row3d.expires_at).toBe(row3d.fetched_at + 21600);

      // Close in 14 days → 24h TTL
      const closeIn14d = report.fetchedAt + 14 * 86400;
      const row14d = client.toDbRow(report, closeIn14d);
      expect(row14d.expires_at).toBe(row14d.fetched_at + 86400);

      // Close in 60 days → 48h TTL
      const closeIn60d = report.fetchedAt + 60 * 86400;
      const row60d = client.toDbRow(report, closeIn60d);
      expect(row60d.expires_at).toBe(row60d.fetched_at + 172800);
    });
  });

  describe('getTtlForCloseTime', () => {
    test('already closed → 1h', () => {
      expect(getTtlForCloseTime(-1000)).toBe(3600);
      expect(getTtlForCloseTime(0)).toBe(3600);
    });

    test('<24h → 1h', () => {
      expect(getTtlForCloseTime(3600)).toBe(3600);      // 1h
      expect(getTtlForCloseTime(86399)).toBe(3600);      // just under 24h
    });

    test('1–7d → 6h', () => {
      expect(getTtlForCloseTime(86400)).toBe(21600);     // exactly 1d
      expect(getTtlForCloseTime(3 * 86400)).toBe(21600); // 3d
      expect(getTtlForCloseTime(7 * 86400 - 1)).toBe(21600); // just under 7d
    });

    test('7–30d → 24h', () => {
      expect(getTtlForCloseTime(7 * 86400)).toBe(86400);  // exactly 7d
      expect(getTtlForCloseTime(15 * 86400)).toBe(86400); // 15d
      expect(getTtlForCloseTime(30 * 86400 - 1)).toBe(86400); // just under 30d
    });

    test('30d+ → 48h', () => {
      expect(getTtlForCloseTime(30 * 86400)).toBe(172800); // exactly 30d
      expect(getTtlForCloseTime(90 * 86400)).toBe(172800); // 90d
    });
  });

  describe('shouldRefresh tiered TTL', () => {
    test('(e) triggers refresh for stale short-dated market', () => {
      const client = new OctagonClient(makeInvoker(REALISTIC_JSON_RESPONSE), db, audit);
      const now = Math.floor(Date.now() / 1000);

      // Insert a report fetched 2 hours ago
      const { insertReport: insert } = require('../../db/octagon-cache.js');
      insert(db, {
        report_id: 'ttl-test-1',
        ticker: 'SHORT-YES',
        event_ticker: 'EV-1',
        model_prob: 0.60,
        market_prob: 0.55,
        fetched_at: now - 7200, // 2h ago
        expires_at: now - 3600, // expired
      });

      // Market closes in 12 hours → 1h TTL → 2h-old report is stale
      const closeIn12h = new Date((now + 12 * 3600) * 1000).toISOString();
      const result = client.shouldRefresh('SHORT-YES', 0.55, false, closeIn12h);
      expect(result.refresh).toBe(true);
      expect(result.reason).toContain('1h TTL tier');
    });

    test('(e) does NOT trigger for fresh long-dated market', () => {
      const client = new OctagonClient(makeInvoker(REALISTIC_JSON_RESPONSE), db, audit);
      const now = Math.floor(Date.now() / 1000);

      // Insert a report fetched 12 hours ago
      const { insertReport: insert } = require('../../db/octagon-cache.js');
      insert(db, {
        report_id: 'ttl-test-2',
        ticker: 'LONG-YES',
        event_ticker: 'EV-1',
        model_prob: 0.60,
        market_prob: 0.55,
        fetched_at: now - 12 * 3600, // 12h ago
        expires_at: now + 36 * 3600,
      });

      // Market closes in 60 days → 48h TTL → 12h-old report is still fresh
      const closeIn60d = new Date((now + 60 * 86400) * 1000).toISOString();
      const result = client.shouldRefresh('LONG-YES', 0.55, false, closeIn60d);
      expect(result.refresh).toBe(false);
    });
  });
});

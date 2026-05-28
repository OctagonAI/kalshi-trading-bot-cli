import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { migrate } from '../../db/schema.js';
import { persistEvent } from '../octagon-prefetch.js';
import type { OctagonEventEntry } from '../octagon-events-api.js';

function makeEvent(overrides: Partial<OctagonEventEntry> = {}): OctagonEventEntry {
  return {
    history_id: 1,
    run_id: 'r1',
    captured_at: '2026-05-27T00:00:00Z',
    event_ticker: 'KXFEDCHAIRNOM-29',
    name: 'Fed Chair',
    slug: 'fed-chair',
    series_category: 'Politics',
    available_on_brokers: true,
    mutually_exclusive: true,
    analysis_last_updated: '2026-05-27T00:00:00Z',
    confidence_score: 8,
    model_probability: 92.1,
    market_probability: 94.0,
    edge_pp: -1.9,
    expected_return: -0.02,
    r_score: 0.5,
    total_volume: 196000,
    total_open_interest: 109000,
    close_time: '2029-01-20T15:00:00Z',
    key_takeaway: 'placeholder',
    ...overrides,
  };
}

describe('persistEvent: per-outcome model_prob in edge_history', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
    migrate(db);
  });
  afterEach(() => {
    db.close();
  });

  test('multi-outcome event writes one edge_history row per outcome', () => {
    persistEvent(db, makeEvent({
      outcome_probabilities: [
        { market_ticker: 'KXFEDCHAIRNOM-29-KW', outcome_name: 'Warsh', model_probability: 86.3, market_probability: 93.5 },
        { market_ticker: 'KXFEDCHAIRNOM-29-JS', outcome_name: 'Shelton', model_probability: 3.8, market_probability: 4.5 },
        { market_ticker: 'KXFEDCHAIRNOM-29-KH', outcome_name: 'Hassett', model_probability: 0.5, market_probability: 0.5 },
      ],
    }));
    const rows = db.query('SELECT ticker, model_prob, market_prob FROM edge_history ORDER BY ticker').all() as Array<{ ticker: string; model_prob: number; market_prob: number }>;
    expect(rows).toHaveLength(3);
    const kw = rows.find((r) => r.ticker === 'KXFEDCHAIRNOM-29-KH');
    expect(kw).toBeDefined();
    expect(rows.find((r) => r.ticker === 'KXFEDCHAIRNOM-29-KW')?.model_prob).toBeCloseTo(0.863, 3);
    expect(rows.find((r) => r.ticker === 'KXFEDCHAIRNOM-29-JS')?.model_prob).toBeCloseTo(0.038, 3);
    // None should be the event-level placeholder 0.921
    for (const r of rows) {
      expect(r.model_prob).not.toBeCloseTo(0.921, 3);
    }
  });

  test('single/no-outcome event falls back to event-level row', () => {
    persistEvent(db, makeEvent({
      event_ticker: 'KXRECSSNBER-26',
      outcome_probabilities: null,
    }));
    const rows = db.query('SELECT ticker, model_prob FROM edge_history').all() as Array<{ ticker: string; model_prob: number }>;
    expect(rows).toHaveLength(1);
    expect(rows[0].ticker).toBe('KXRECSSNBER-26');
    expect(rows[0].model_prob).toBeCloseTo(0.921, 3);
  });

  test('outcomes missing required fields are skipped', () => {
    persistEvent(db, makeEvent({
      outcome_probabilities: [
        { market_ticker: 'KX-OK', model_probability: 50, market_probability: 48 },
        // missing market_ticker — should be skipped
        { model_probability: 10, market_probability: 10 } as any,
        // missing prob — should be skipped
        { market_ticker: 'KX-MISSING-PROB' } as any,
      ],
    }));
    const rows = db.query('SELECT ticker FROM edge_history').all() as Array<{ ticker: string }>;
    expect(rows).toHaveLength(1);
    expect(rows[0].ticker).toBe('KX-OK');
  });
});

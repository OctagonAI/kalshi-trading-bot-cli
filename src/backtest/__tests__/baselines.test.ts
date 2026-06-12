import { describe, test, expect } from 'bun:test';
import { computeMetrics } from '../metrics.js';
import type { ScoredSignal } from '../types.js';

function signal(overrides: Partial<ScoredSignal>): ScoredSignal {
  return {
    event_ticker: 'KX-EVT',
    market_ticker: 'KX-EVT-A',
    series_category: 'Test',
    model_prob: 60,
    market_then: 50,
    market_now: 100,        // resolved YES
    resolved: true,
    edge_pp: 10,            // model > market → YES bet
    pnl: 0.5,
    capital: 0.5,
    edge_bucket: '5-10%',
    confidence_score: 5,
    close_time: '2026-06-01T00:00:00Z',
    ...overrides,
  };
}

describe('computeBaselines — null strategies on the same universe', () => {
  test('always-NO ROI is positive when most signals settle NO', () => {
    // NO bet at market_then=X: capital=(100-X)/100, payout=$1 if NO wins,
    // so pnl = X/100 when NO wins, -(100-X)/100 when NO loses.
    const sigs: ScoredSignal[] = [
      // 3 contracts settle NO (model bet YES = wrong direction; not used for baseline math)
      signal({ market_ticker: 'A', market_then: 50, market_now: 0, edge_pp: 10, pnl: -0.5, capital: 0.5 }),
      signal({ market_ticker: 'B', market_then: 30, market_now: 0, edge_pp: 5,  pnl: -0.3, capital: 0.3 }),
      signal({ market_ticker: 'C', market_then: 20, market_now: 0, edge_pp: 5,  pnl: -0.2, capital: 0.2 }),
      // 1 contract settles YES
      signal({ market_ticker: 'D', market_then: 60, market_now: 100, edge_pp: 5, pnl: 0.4, capital: 0.6 }),
    ];
    const m = computeMetrics(sigs);
    // Always-NO PnL per signal:
    //   A (50, 0):   cap 0.5, pnl +0.5
    //   B (30, 0):   cap 0.7, pnl +0.3
    //   C (20, 0):   cap 0.8, pnl +0.2
    //   D (60,100):  cap 0.4, pnl -0.4
    //   sum pnl = 0.5 + 0.3 + 0.2 - 0.4 = 0.6
    //   sum cap = 0.5 + 0.7 + 0.8 + 0.4 = 2.4
    //   ROI     = 0.6 / 2.4 = +25.0%
    expect(m.baselines.always_no_roi).toBeCloseTo(0.6 / 2.4, 3);
    expect(m.baselines.always_no_hit_rate).toBe(0.75);
  });

  test('always-YES underperforms when the universe is NO-heavy', () => {
    const sigs: ScoredSignal[] = [
      signal({ market_ticker: 'A', market_then: 50, market_now: 0, edge_pp: 10, pnl: -0.5, capital: 0.5 }),
      signal({ market_ticker: 'B', market_then: 30, market_now: 0, edge_pp: 5,  pnl: -0.3, capital: 0.3 }),
      signal({ market_ticker: 'C', market_then: 60, market_now: 100, edge_pp: 5, pnl: 0.4, capital: 0.6 }),
    ];
    const m = computeMetrics(sigs);
    // Always-YES PnL per signal:
    //   A (50, 0):   cap 0.5, pnl -0.5
    //   B (30, 0):   cap 0.3, pnl -0.3
    //   C (60,100):  cap 0.6, pnl +0.4
    //   sum pnl = -0.4, sum cap = 1.4, ROI ≈ -28.6%
    expect(m.baselines.always_yes_roi).toBeCloseTo(-0.4 / 1.4, 3);
    expect(m.baselines.always_no_roi).toBeGreaterThan(m.baselines.always_yes_roi);
  });

  test('within-band skill = 0 when model bets NO at the same prices as always-NO would', () => {
    // Model bets NO on every contract in 20-40 band, all win.
    // Per-signal NO bet at 30: cap=0.7, pnl=0.3. At 25: cap=0.75, pnl=0.25.
    // Always-NO baseline produces the same returns since the model is just
    // always-NO restricted to this band. Skill delta should be 0pp.
    const sigs: ScoredSignal[] = [
      signal({ market_ticker: 'A', market_then: 30, market_now: 0, edge_pp: -10, pnl: 0.3,  capital: 0.7 }),
      signal({ market_ticker: 'B', market_then: 25, market_now: 0, edge_pp: -8,  pnl: 0.25, capital: 0.75 }),
    ];
    const m = computeMetrics(sigs);
    expect(m.baselines.within_band_skill_pp).toBeCloseTo(0, 5);
  });

  test('within-band breakdown has the right population counts', () => {
    const sigs: ScoredSignal[] = [
      // 20-40 band, model picks NO on 1 of 2 contracts
      signal({ market_ticker: 'A', market_then: 30, market_now: 0, edge_pp: -5, pnl: 0.7, capital: 0.7 }),
      signal({ market_ticker: 'B', market_then: 25, market_now: 0, edge_pp: 3, pnl: -0.25, capital: 0.25 }),
      // 60-80 band, model picks NO on 1 contract
      signal({ market_ticker: 'C', market_then: 70, market_now: 0, edge_pp: -8, pnl: 0.3, capital: 0.3 }),
    ];
    const m = computeMetrics(sigs);
    const band2040 = m.baselines.within_band_breakdown.find((b) => b.band === '20-40¢');
    const band6080 = m.baselines.within_band_breakdown.find((b) => b.band === '60-80¢');
    expect(band2040?.n_universe).toBe(2);
    expect(band2040?.n_model).toBe(1);
    expect(band6080?.n_universe).toBe(1);
    expect(band6080?.n_model).toBe(1);
  });

  test('empty signal list produces empty baselines without dividing by zero', () => {
    const m = computeMetrics([]);
    expect(m.baselines.always_no_roi).toBe(0);
    expect(m.baselines.always_yes_roi).toBe(0);
    expect(m.baselines.within_band_skill_pp).toBe(0);
    expect(m.baselines.within_band_breakdown).toEqual([]);
  });
});

describe('Edge filter — unrounded |edge| (Issue 8)', () => {
  test('signal with unrounded edge_pp = 0.45 is EXCLUDED when minEdgePp = 0.5', () => {
    // Before the fix: 0.45 → rounded to 0.5 (≥ 0.5) → INCLUDED.
    // After the fix:  0.45 stays 0.45, < 0.5, → EXCLUDED.
    const s: ScoredSignal[] = [
      signal({ market_ticker: 'A', edge_pp: 0.45, resolved: true, market_then: 50, market_now: 100, pnl: 0.5, capital: 0.5 }),
    ];
    const m = computeMetrics(s, 0.5);
    expect(m.edge_signals).toBe(0);
  });

  test('signal with unrounded edge_pp = 0.55 is INCLUDED when minEdgePp = 0.5', () => {
    const s: ScoredSignal[] = [
      signal({ market_ticker: 'A', edge_pp: 0.55, resolved: true, market_then: 50, market_now: 100, pnl: 0.5, capital: 0.5 }),
    ];
    const m = computeMetrics(s, 0.5);
    expect(m.edge_signals).toBe(1);
  });

  test('edge_pp = 0 is always excluded from edge metrics', () => {
    const s: ScoredSignal[] = [
      signal({ market_ticker: 'A', edge_pp: 0, resolved: true, market_then: 50, market_now: 50, pnl: 0, capital: 0.5 }),
    ];
    const m = computeMetrics(s, 0);
    expect(m.edge_signals).toBe(0);
  });
});

describe('LegMetrics — resolved vs unresolved split (Issue 3)', () => {
  test('resolved_metrics covers only resolved signals; unresolved_metrics only unresolved', () => {
    const sigs: ScoredSignal[] = [
      // Two resolved NO winners — same as before
      signal({ market_ticker: 'A', resolved: true,  market_then: 30, market_now: 0,  edge_pp: -10, pnl: 0.3,  capital: 0.7 }),
      signal({ market_ticker: 'B', resolved: true,  market_then: 25, market_now: 0,  edge_pp: -8,  pnl: 0.25, capital: 0.75 }),
      // One unresolved that drifted the wrong way
      signal({ market_ticker: 'C', resolved: false, market_then: 50, market_now: 30, edge_pp: 5,   pnl: -0.20, capital: 0.5 }),
    ];
    const m = computeMetrics(sigs);
    expect(m.resolved_metrics.edge_signals).toBe(2);
    expect(m.unresolved_metrics.edge_signals).toBe(1);
    // Resolved ROI = (0.3 + 0.25) / (0.7 + 0.75) ≈ +37.9%
    expect(m.resolved_metrics.flat_bet_roi).toBeCloseTo(0.55 / 1.45, 3);
    // Unresolved is a loser
    expect(m.unresolved_metrics.flat_bet_roi).toBeLessThan(0);
    // Top-level fields still carry the blended view (backward compat)
    expect(m.edge_signals).toBe(3);
  });

  test('all-resolved input yields empty unresolved_metrics', () => {
    const sigs: ScoredSignal[] = [
      signal({ market_ticker: 'A', resolved: true, market_then: 30, market_now: 0, edge_pp: -10, pnl: 0.3, capital: 0.7 }),
    ];
    const m = computeMetrics(sigs);
    expect(m.resolved_metrics.edge_signals).toBe(1);
    expect(m.unresolved_metrics.edge_signals).toBe(0);
    expect(m.unresolved_metrics.total_capital).toBe(0);
  });
});
